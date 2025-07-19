use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple, PyList};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::config::ClientConfig;
use rdkafka::message::Message;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use ta::indicators::BollingerBands;
use ta::Next;
use chrono::{DateTime, Utc, Duration};

#[pyclass]
struct IndicatorEngine {
    config: PyObject,
    adapters: PyObject,
    watchlists: PyObject,
    producer: Arc<FutureProducer>,
    consumer: Arc<StreamConsumer>,
    offline_mode: bool,
    ohlcv_state: Arc<Mutex<HashMap<String, HashMap<String, Vec<Value>>>>>,
    bb_state: Arc<Mutex<HashMap<String, HashMap<String, BollingerBands>>>>,
}

#[pymethods]
impl IndicatorEngine {
    #[new]
    fn new(config: PyObject, adapters: PyObject, watchlists: PyObject) -> PyResult<Self> {
        let (offline_mode, kafka_brokers) = Python::with_gil(|py| -> PyResult<(bool, String)> {
            let config_dict = config.extract::<&PyDict>(py)?;
            let global_dict = config_dict.get_item("global")?.ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("Missing 'global' in config"))?.extract::<&PyDict>()?;
            let offline = global_dict.get_item("offline_mode")?.map_or(false, |v| v.extract::<bool>().unwrap_or(false));
            let kafka = global_dict.get_item("kafka")?.extract::<&PyDict>()?.get_item("brokers")?.extract::<String>()?;
            Ok((offline, kafka))
        })?;

        let producer = Arc::new(ClientConfig::new()
            .set("bootstrap.servers", &kafka_brokers)
            .create()
            .expect("Producer creation failed"));

        let consumer = Arc::new(ClientConfig::new()
            .set("bootstrap.servers", &kafka_brokers)
            .set("group.id", "anquant")
            .set("auto.offset.reset", "latest")
            .create()
            .expect("Consumer creation failed"));

        Ok(IndicatorEngine {
            config,
            adapters,
            watchlists,
            producer,
            consumer,
            offline_mode,
            ohlcv_state: Arc::new(Mutex::new(HashMap::new())),
            bb_state: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn initialize<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let adapters = self.adapters.clone();
        let watchlists = self.watchlists.clone();
        let offline_mode = self.offline_mode;
        let bb_state = self.bb_state.clone();

        pyo3_asyncio_0_21::tokio::local_future_into_py(py, async move {
            Python::with_gil(|py| -> PyResult<()> {
                let adapters_dict = adapters.extract::<&PyDict>(py)?;
                for (_, brokers) in adapters_dict.iter() {
                    let brokers_dict = brokers.extract::<&PyDict>()?;
                    for (_, adapter) in brokers_dict.iter() {
                        adapter.getattr("connect")?.call0()?;
                    }
                }
                Ok(())
            })?;

            if !offline_mode {
                Python::with_gil(|py| -> PyResult<()> {
                    let watchlists_dict = watchlists.extract::<&PyDict>(py)?;
                    for (_, watchlist) in watchlists_dict.iter() {
                        let symbols: Vec<String> = watchlist.extract()?;
                        for symbol in symbols {
                            let hist_module = py.import_bound("src.py.core.historical_data_manager")?;
                            for timeframe in ["1min", "5min", "30min"] {
                                let hist_list_obj = hist_module.getattr("load_historical_data")?.call1((symbol.clone(), timeframe.to_string()))?;
                                let hist_list = hist_list_obj.extract::<&PyList>(py)?;
                                let mut bb = BollingerBands::new(20, 2.0).expect("Failed to initialize Bollinger Bands");
                                for candle in hist_list.iter() {
                                    let close = candle.getattr("close")?.extract::<f64>()?;
                                    bb.next(close);
                                }
                                bb_state.lock().unwrap().entry(symbol.clone()).or_insert_with(HashMap::new).insert(timeframe.to_string(), bb);
                            }
                        }
                    }
                    Ok(())
                })?;
            }

            println!("IndicatorEngine initialized");
            Ok(())
        })
    }

    fn start<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let adapters = self.adapters.clone();
        let watchlists = self.watchlists.clone();
        let offline_mode = self.offline_mode;
        let consumer = self.consumer.clone();
        let producer = self.producer.clone();
        let ohlcv_state = self.ohlcv_state.clone();
        let bb_state = self.bb_state.clone();

        pyo3_asyncio_0_21::tokio::local_future_into_py(py, async move {
            let market_map = Python::with_gil(|py| -> PyResult<HashMap<String, (HashMap<String, PyObject>, Vec<String>)>> {
                let adapters_dict = adapters.extract::<&PyDict>(py)?;
                let watchlists_dict = watchlists.extract::<&PyDict>(py)?;
                let mut market_map = HashMap::new();

                for (market, brokers) in adapters_dict.iter() {
                    let market_str: String = market.extract()?;
                    let brokers_dict = brokers.extract::<&PyDict>()?;
                    let symbols: Vec<String> = watchlists_dict
                        .get_item(market)?
                        .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(format!("Missing watchlist for market: {}", market_str)))?
                        .extract()?;

                    let mut broker_map = HashMap::new();
                    for (broker, adapter) in brokers_dict.iter() {
                        broker_map.insert(broker.extract()?, adapter.to_object(py));
                    }

                    market_map.insert(market_str, (broker_map, symbols));
                }

                Ok(market_map)
            })?;

            if !offline_mode {
                Python::with_gil(|py| -> PyResult<()> {
                    for (market, (brokers, symbols)) in market_map.iter() {
                        for (broker, adapter_obj) in brokers {
                            let adapter = adapter_obj.bind(py);
                            let subscribe = adapter.getattr("subscribe_to_ticks")?.call1((symbols.clone(),))?;
                            pyo3_asyncio_0_21::tokio::into_future(subscribe)?.await?;
                            println!("Subscribed to market: {}, broker: {}", market, broker);
                        }
                    }
                    Ok(())
                })?;

                consumer.subscribe(&["nse_ticks"]).expect("Failed to subscribe to nse_ticks");
                let mut message_stream = consumer.stream();
                while let Some(result) = message_stream.next().await {
                    if let Ok(msg) = result {
                        let tick: Value = serde_json::from_slice(msg.payload().unwrap_or(&[])).unwrap_or_default();
                        let symbol = tick["tradingsymbol"].as_str().unwrap_or("").to_string();
                        let timestamp_str = tick["timestamp"].as_str().unwrap_or("");
                        let timestamp = DateTime::parse_from_rfc3339(timestamp_str)
                            .map(|dt| dt.with_timezone(&Utc))
                            .unwrap_or(Utc::now());

                        for timeframe in ["1min", "5min", "30min"] {
                            let ohlcv = {
                                let mut ohlcv_guard = ohlcv_state.lock().unwrap();
                                Self::aggregate_ohlcv(&mut *ohlcv_guard, &symbol, timeframe, &tick, timestamp)
                            };

                            let close = ohlcv["close"].as_f64().unwrap_or(0.0);
                            let indicators = {
                                let mut bb_guard = bb_state.lock().unwrap();
                                let symbol_bb = bb_guard.entry(symbol.clone()).or_insert_with(HashMap::new);
                                let bb = symbol_bb.entry(timeframe.to_string()).or_insert_with(|| BollingerBands::new(20, 2.0).expect("Failed to initialize Bollinger Bands"));
                                let result = bb.next(close);
                                json!({
                                    "bb_upper": result.upper,
                                    "bb_average": result.average,
                                    "bb_lower": result.lower
                                })
                            };

                            Python::with_gil(|py| -> PyResult<()> {
                                let redis_client_module = py.import_bound("src.py.messaging.redis_client")?;
                                let redis_client = redis_client_module.getattr("RedisClient")?.call0()?;
                                let key_ohlcv = format!("{}:ohlcv:{}", symbol, timeframe);
                                let key_indicators = format!("{}:indicators:{}", symbol, timeframe);
                                let ohlcv_dict = Self::value_to_pydict(py, &ohlcv)?;
                                let args_ohlcv = PyTuple::new_bound(py, &[key_ohlcv.as_str(), ohlcv_dict.as_any(), 86400]);
                                let indicators_dict = Self::value_to_pydict(py, &indicators)?;
                                let args_indicators = PyTuple::new_bound(py, &[key_indicators.as_str(), indicators_dict.as_any(), 86400]);
                                redis_client.call_method_bound("cache", &args_ohlcv, None)?;
                                redis_client.call_method_bound("cache", &args_indicators, None)?;

                                let database_module = py.import_bound("src.py.util.database")?;
                                let database = database_module.getattr("Database")?.call0()?;
                                let args_db = PyTuple::new_bound(py, &[symbol.as_str(), timeframe, ohlcv_dict.as_any()]);
                                database.call_method_bound("save_ohlcv", &args_db, None)?;

                                Ok(())
                            })?;

                            producer
                                .send(
                                    FutureRecord::to(&format!("ohlcv_{}", timeframe))
                                        .key(&symbol)
                                        .payload(&serde_json::to_string(&ohlcv).unwrap()),
                                    std::time::Duration::from_secs(0),
                                )
                                .await
                                .expect("Failed to publish to ohlcv");
                        }
                    }
                }
            } else {
                println!("Offline mode: Skipping subscriptions and tick processing");
            }

            println!("IndicatorEngine started");
            Ok(())
        })
    }

    fn stop<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let adapters = self.adapters.clone();

        pyo3_asyncio_0_21::tokio::local_future_into_py(py, async move {
            Python::with_gil(|py| -> PyResult<()> {
                let adapters_dict = adapters.extract::<&PyDict>(py)?;
                for (_, brokers) in adapters_dict.iter() {
                    let brokers_dict = brokers.extract::<&PyDict>()?;
                    for (_, adapter) in brokers_dict.iter() {
                        let disconnect = adapter.getattr("disconnect")?.call0()?;
                        pyo3_asyncio_0_21::tokio::into_future(disconnect)?.await?;
                    }
                }
                Ok(())
            })?;
            println!("IndicatorEngine stopped");
            Ok(())
        })
    }
}

impl IndicatorEngine {
    fn aggregate_ohlcv(
        state: &mut HashMap<String, HashMap<String, Vec<Value>>>,
        symbol: &str,
        timeframe: &str,
        tick: &Value,
        timestamp: DateTime<Utc>,
    ) -> Value {
        let duration = match timeframe {
            "1min" => Duration::minutes(1),
            "5min" => Duration::minutes(5),
            "30min" => Duration::minutes(30),
            _ => Duration::minutes(1),
        };
        let rounded_timestamp = timestamp - Duration::milliseconds(timestamp.timestamp_millis() % duration.num_milliseconds());

        let symbol_state = state.entry(symbol.to_string()).or_insert_with(HashMap::new);
        let timeframe_state = symbol_state.entry(timeframe.to_string()).or_insert_with(Vec::new);

        let ltp = tick["ltp"].as_f64().unwrap_or(0.0);
        let volume = tick.get("volume").and_then(|v| v.as_i64()).unwrap_or(1);

        if let Some(last_ohlcv) = timeframe_state.last_mut() {
            let last_timestamp: DateTime<Utc> = DateTime::parse_from_rfc3339(last_ohlcv["timestamp"].as_str().unwrap())
                .expect("Failed to parse last timestamp")
                .with_timezone(&Utc);
            if last_timestamp == rounded_timestamp {
                last_ohlcv["high"] = json!(last_ohlcv["high"].as_f64().unwrap().max(ltp));
                last_ohlcv["low"] = json!(last_ohlcv["low"].as_f64().unwrap().min(ltp));
                last_ohlcv["close"] = json!(ltp);
                last_ohlcv["volume"] = json!(last_ohlcv["volume"].as_i64().unwrap() + volume);
                return last_ohlcv.clone();
            }
        }

        let ohlcv = json!({
            "tradingsymbol": symbol,
            "open": ltp,
            "high": ltp,
            "low": ltp,
            "close": ltp,
            "volume": volume,
            "timestamp": rounded_timestamp.to_rfc3339(),
            "exchange": "NSE"
        });
        timeframe_state.push(ohlcv.clone());
        ohlcv
    }

    fn value_to_pydict(py: Python, value: &Value) -> PyResult<PyObject> {
        let dict = PyDict::new_bound(py);
        if let Value::Object(map) = value {
            for (k, v) in map {
                match v {
                    Value::Null => dict.set_item(k, py.None())?,
                    Value::Bool(b) => dict.set_item(k, *b)?,
                    Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            dict.set_item(k, i)?;
                        } else if let Some(f) = n.as_f64() {
                            dict.set_item(k, f)?;
                        }
                    }
                    Value::String(s) => dict.set_item(k, s)?,
                    Value::Array(arr) => {
                        let py_list = arr.iter().map(|v| Self::value_to_pydict(py, v)).collect::<PyResult<Vec<_>>>()?;
                        dict.set_item(k, py_list)?;
                    }
                    Value::Object(_) => {
                        let nested_dict = Self::value_to_pydict(py, v)?;
                        dict.set_item(k, nested_dict)?;
                    }
                }
            }
        }
        Ok(dict.into_any().unbind().to_object(py))
    }
}

#[pymodule]
fn indicator_engine(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<IndicatorEngine>()?;
    Ok(())
}