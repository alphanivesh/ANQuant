use pyo3::prelude::*;
use pyo3::types::PyDict;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::config::ClientConfig;
use std::collections::HashMap;

#[pyclass]
struct MarketDataEngine {
    config: PyObject,
    adapters: PyObject,
    watchlists: PyObject,
    producer: FutureProducer,
    offline_mode: bool,
}

#[pymethods]
impl MarketDataEngine {
    #[new]
    fn new(config: PyObject, adapters: PyObject, watchlists: PyObject) -> PyResult<Self> {
        let (offline_mode, kafka_brokers) = Python::with_gil(|py| -> PyResult<(bool, String)> {
            let config_dict = config.extract::<&PyDict>(py)?;
            let global_dict = config_dict.get_item("global")?.ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("Missing 'global' in config"))?.extract::<&PyDict>()?;
            let offline = global_dict.get_item("offline_mode")?.map_or(false, |v| v.extract::<bool>().unwrap_or(false));
            let kafka = global_dict.get_item("kafka")?.extract::<&PyDict>()?.get_item("brokers")?.extract::<String>()?;
            Ok((offline, kafka))
        })?;

        let producer = ClientConfig::new()
            .set("bootstrap.servers", &kafka_brokers)
            .create()
            .expect("Producer creation failed");

        Ok(MarketDataEngine {
            config,
            adapters,
            watchlists,
            producer,
            offline_mode,
        })
    }

    fn initialize<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let adapters = self.adapters.clone();

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
            println!("MarketDataEngine initialized");
            Ok(())
        })
    }

    fn start<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let adapters = self.adapters.clone();
        let watchlists = self.watchlists.clone();
        let offline_mode = self.offline_mode;
        let producer = self.producer.clone();

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
                    for (market, (brokers, symbols)) in market_map {
                        for (broker, adapter_obj) in brokers {
                            let adapter = adapter_obj.bind(py);
                            let subscribe = adapter.getattr("subscribe_to_ticks")?.call1((symbols.clone(),))?;
                            pyo3_asyncio_0_21::tokio::into_future(subscribe)?.await?;
                            println!("Subscribed to market: {}, broker: {}", market, broker);
                        }
                    }
                    Ok(())
                })?;
            } else {
                println!("Offline mode: Skipping subscriptions");
            }
            println!("MarketDataEngine started");
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
            println!("MarketDataEngine stopped");
            Ok(())
        })
    }
}

#[pymodule]
fn market_data_engine(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<MarketDataEngine>()?;
    Ok(())
}