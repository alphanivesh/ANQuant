# D:\AlphaNivesh\ANQuant\src\anquant\py\core\flexirule\rule_engine.py
import asyncio
import json
from typing import Dict, Any, Optional, List
import operator
import pandas as pd
import pandas_ta as pta
from src.anquant.py.util.logging import setup_logging
from src.anquant.py.messaging.redis_client import RedisClient
from src.anquant.py.util.database import Database
import asteval

logger = setup_logging("rule_engine", log_type="strategy")

class RuleEngine:
    def __init__(self, strategy_config: Dict[str, Any], redis_client: RedisClient):
        self.strategy_config = strategy_config
        self.name = strategy_config['name']
        self.timeframe = strategy_config['timeframe']
        self.threshold = strategy_config.get('threshold', 0.75)
        self.indicators = strategy_config.get('indicators', [])
        self.patterns = strategy_config.get('patterns', [])
        self.entry_rules = strategy_config.get('entry_rules', [])
        self.exit_rules = strategy_config.get('exit_rules', [])
        self.stop_loss = strategy_config.get('stop_loss', {})
        self.target = strategy_config.get('target', {})
        self.trade_management = strategy_config.get('trade_management', {})
        self.market_params = strategy_config.get('market_params', {}).get('india', {})
        self.redis_client = redis_client
        self.database = Database(strategy_config.get('database', {}))
        self.positions = {}  # symbol: {entry_price, highest_price, lowest_price, breakeven_triggered}
        self.pattern_cache = {}  # symbol: {pattern_name: bool}
        self.interpreter = asteval.Interpreter()
        logger.debug(f"Initialized RuleEngine for strategy {self.name}")

    async def initialize(self):
        """Initialize Redis and database connections."""
        logger.info(f"Initializing RuleEngine for strategy {self.name}")
        try:
            await self.redis_client.connect()
            await self.redis_client.redis.ping()
            logger.debug(f"Verified Redis connectivity for RuleEngine {self.name}")
            await self.database.connect()
            for rule in self.entry_rules + self.exit_rules:
                if 'condition' not in rule or 'weight' not in rule:
                    logger.error(f"Invalid rule format in strategy {self.name}: {rule}")
                    raise ValueError(f"Invalid rule format in strategy {self.name}")
            for indicator in self.indicators:
                if 'type' not in indicator or 'name' not in indicator:
                    logger.error(f"Invalid indicator format in strategy {self.name}: {indicator}")
                    raise ValueError(f"Invalid indicator format in strategy {self.name}")
            for pattern in self.patterns:
                if 'type' not in pattern or 'name' not in pattern:
                    logger.error(f"Invalid pattern format in strategy {self.name}: {pattern}")
                    raise ValueError(f"Invalid pattern format in strategy {self.name}")
            logger.info(f"RuleEngine for {self.name} initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize RuleEngine for {self.name}: {e}", exc_info=True)
            raise

    async def evaluate(self, symbol: str, ohlcv: Dict, indicators: Dict) -> Optional[str]:
        """Evaluate rules and generate trading signal."""
        try:
            position = self.positions.get(symbol, {
                'entry_price': None,
                'highest_price': None,
                'lowest_price': None,
                'breakeven_triggered': False
            })
            close = ohlcv['close']

            # Update pattern cache
            await self._update_patterns(symbol, ohlcv)

            # Check stop-loss and target for open position
            if position['entry_price'] is not None:
                # Check breakeven
                breakeven_signal = self._evaluate_breakeven(symbol, close, position)
                if breakeven_signal:
                    position['breakeven_triggered'] = True
                    self.positions[symbol] = position

                sl_signal = await self._evaluate_stop_loss(symbol, close, position)
                if sl_signal:
                    self.positions.pop(symbol, None)
                    await self._log_audit_trail(symbol, ohlcv, indicators, sl_signal, f"Stop-loss triggered ({sl_signal})")
                    return sl_signal
                target_signal = await self._evaluate_target(symbol, close, position)
                if target_signal:
                    if target_signal.startswith("PARTIAL_SELL"):
                        position['quantity'] = position.get('quantity', 100) * (1 - float(target_signal.split(':')[1]) / 100)
                        self.positions[symbol] = position
                    else:
                        self.positions.pop(symbol, None)
                    await self._log_audit_trail(symbol, ohlcv, indicators, target_signal, f"Target triggered ({target_signal})")
                    return target_signal
                exit_signal = self._evaluate_rules(self.exit_rules, ohlcv, indicators)
                if exit_signal:
                    self.positions.pop(symbol, None)
                    await self._log_audit_trail(symbol, ohlcv, indicators, "SELL", "Exit rule triggered")
                    return "SELL"

            # Check entry rules
            entry_signal = self._evaluate_rules(self.entry_rules, ohlcv, indicators)
            if entry_signal:
                self.positions[symbol] = {
                    'entry_price': close,
                    'highest_price': close,
                    'lowest_price': close,
                    'breakeven_triggered': False,
                    'quantity': 100  # Default quantity
                }
                await self._log_audit_trail(symbol, ohlcv, indicators, "BUY", "Entry rule triggered")
                return "BUY"

            return "HOLD"
        except Exception as e:
            logger.error(f"Error evaluating rules for {symbol} in {self.name}: {e}", exc_info=True)
            return "HOLD"

    async def _update_patterns(self, symbol: str, ohlcv: Dict):
        """Update pattern cache for the symbol."""
        try:
            for pattern in self.patterns:
                pattern_name = pattern['name']
                pattern_type = pattern['type']
                lookback = pattern.get('lookback', 20)
                criteria = pattern.get('criteria', '')

                historical = await self.redis_client.get(f"{symbol}:ohlcv:{self.timeframe}")
                if not historical or len(historical) < lookback:
                    logger.debug(f"Insufficient historical data for pattern {pattern_name} on {symbol}")
                    self.pattern_cache.setdefault(symbol, {})[pattern_name] = False
                    continue

                df = pd.DataFrame(historical[-lookback:])
                if pattern_type == 'smc':
                    self.pattern_cache.setdefault(symbol, {})[pattern_name] = self._evaluate_smc_pattern(df, criteria)
                elif pattern_type == 'price_action':
                    self.pattern_cache.setdefault(symbol, {})[pattern_name] = self._evaluate_price_action_pattern(df, criteria)
                elif pattern_type == 'harmonic':
                    self.pattern_cache.setdefault(symbol, {})[pattern_name] = self._evaluate_harmonic_pattern(df, criteria)
                elif pattern_type == 'wave':
                    self.pattern_cache.setdefault(symbol, {})[pattern_name] = self._evaluate_wave_pattern(df, criteria)
                else:
                    logger.error(f"Unsupported pattern type {pattern_type} for {pattern_name}")
                    self.pattern_cache.setdefault(symbol, {})[pattern_name] = False
        except Exception as e:
            logger.error(f"Error updating patterns for {symbol}: {e}", exc_info=True)
            self.pattern_cache.setdefault(symbol, {})[pattern_name] = False

    def _evaluate_smc_pattern(self, df: pd.DataFrame, criteria: str) -> bool:
        """Evaluate SMC patterns (e.g., order blocks)."""
        try:
            if 'order_block' in criteria:
                prev_high = df['high'].shift(1).iloc[-1]
                prev_low = df['low'].shift(1).iloc[-1]
                volume = df['volume'].iloc[-1]
                avg_volume = df['volume'].rolling(20).mean().iloc[-1]
                expr = criteria.replace('prev_high', str(prev_high)) \
                              .replace('prev_low', str(prev_low)) \
                              .replace('volume', str(volume)) \
                              .replace('avg_volume_20', str(avg_volume))
                return self.interpreter(expr)
            return False
        except Exception as e:
            logger.error(f"Error evaluating SMC pattern {criteria}: {e}", exc_info=True)
            return False

    def _evaluate_price_action_pattern(self, df: pd.DataFrame, criteria: str) -> bool:
        """Evaluate price action patterns (e.g., bullish engulfing)."""
        try:
            if 'bullish_engulfing' in criteria:
                curr = df.iloc[-1]
                prev = df.iloc[-2]
                return (curr['close'] > curr['open'] and
                        prev['close'] < prev['open'] and
                        curr['close'] > prev['high'] and
                        curr['open'] < prev['low'])
            return False
        except Exception as e:
            logger.error(f"Error evaluating price action pattern {criteria}: {e}", exc_info=True)
            return False

    def _evaluate_harmonic_pattern(self, df: pd.DataFrame, criteria: str) -> bool:
        """Evaluate harmonic patterns (e.g., Gartley)."""
        try:
            # Placeholder: Implement harmonic pattern detection
            if 'gartley' in criteria:
                logger.warning("Harmonic pattern detection not fully implemented")
                return False
            return False
        except Exception as e:
            logger.error(f"Error evaluating harmonic pattern {criteria}: {e}", exc_info=True)
            return False

    def _evaluate_wave_pattern(self, df: pd.DataFrame, criteria: str) -> bool:
        """Evaluate wave patterns (e.g., Elliott Wave)."""
        try:
            # Placeholder: Implement wave pattern detection
            if 'elliott_wave' in criteria:
                logger.warning("Wave pattern detection not fully implemented")
                return False
            return False
        except Exception as e:
            logger.error(f"Error evaluating wave pattern {criteria}: {e}", exc_info=True)
            return False

    def _evaluate_rules(self, rules: List[Dict], ohlcv: Dict, indicators: Dict) -> bool:
        """Evaluate a list of rules and return True if conditions are met."""
        try:
            total_weight = 0.0
            required_weight = sum(rule['weight'] for rule in rules) * self.threshold
            for rule in rules:
                condition = rule['condition']
                weight = rule['weight']
                if self._evaluate_condition(condition, ohlcv, indicators):
                    total_weight += weight
            return total_weight >= required_weight
        except Exception as e:
            logger.error(f"Error evaluating rules for {self.name}: {e}", exc_info=True)
            return False

    def _evaluate_condition(self, condition: str, ohlcv: Dict, indicators: Dict) -> bool:
        """Evaluate a single condition (e.g., 'close < bb_lower and rsi < 30')."""
        try:
            # Handle pattern-based conditions
            for pattern in self.patterns:
                pattern_name = pattern['name']
                if f"{pattern_name} = true" in condition.lower():
                    return self.pattern_cache.get(ohlcv['tradingsymbol'], {}).get(pattern_name, False)

            # Handle market parameters (e.g., volume_threshold)
            condition = self._replace_market_params(condition, ohlcv, indicators)

            # Handle complex conditions with 'and'/'or'
            if ' and ' in condition.lower():
                conditions = condition.split(' and ')
                return all(self._evaluate_single_condition(c.strip(), ohlcv, indicators) for c in conditions)
            elif ' or ' in condition.lower():
                conditions = condition.split(' or ')
                return any(self._evaluate_single_condition(c.strip(), ohlcv, indicators) for c in conditions)
            else:
                return self._evaluate_single_condition(condition, ohlcv, indicators)
        except Exception as e:
            logger.error(f"Error evaluating condition {condition}: {e}", exc_info=True)
            return False

    def _replace_market_params(self, condition: str, ohlcv: Dict, indicators: Dict) -> str:
        """Replace market parameters (e.g., volume_threshold) with evaluated values."""
        try:
            for param, expr in self.market_params.items():
                if param in condition:
                    value = self.interpreter(expr, {'avg_volume_20': indicators.get('avg_volume_20', 0.0)})
                    condition = condition.replace(param, str(value))
            return condition
        except Exception as e:
            logger.error(f"Error replacing market params in {condition}: {e}", exc_info=True)
            return condition

    def _evaluate_single_condition(self, condition: str, ohlcv: Dict, indicators: Dict) -> bool:
        """Evaluate a single condition (e.g., 'close < bb_lower')."""
        try:
            left, op, right = self._parse_condition(condition)
            left_value = ohlcv.get(left, indicators.get(left, 0.0))
            right_value = indicators.get(right, ohlcv.get(right, float(right)))
            operators = {
                '>': operator.gt,
                '<': operator.lt,
                '=': operator.eq,
                '>=': operator.ge,
                '<=': operator.le,
                '!=': operator.ne
            }
            if op not in operators:
                logger.error(f"Unsupported operator in condition: {condition}")
                return False
            return operators[op](float(left_value), float(right_value))
        except Exception as e:
            logger.error(f"Error evaluating single condition {condition}: {e}", exc_info=True)
            return False

    def _parse_condition(self, condition: str) -> tuple:
        """Parse a condition string into left, operator, right."""
        for op in ['>=', '<=', '!=', '>', '<', '=']:
            if op in condition:
                left, right = condition.split(op)
                return left.strip(), op, right.strip()
        raise ValueError(f"Invalid condition format: {condition}")

    async def _evaluate_stop_loss(self, symbol: str, close: float, position: Dict) -> Optional[str]:
        """Evaluate stop-loss conditions."""
        try:
            if not self.stop_loss:
                return None
            entry_price = position['entry_price']
            if self.stop_loss['type'] == 'multi':
                for rule in self.stop_loss.get('rules', []):
                    if rule['type'] == 'fixed':
                        sl_value = float(rule['value'].strip('%')) / 100
                        sl_price = entry_price * (1 - sl_value)
                        if close <= sl_price:
                            logger.info(f"Stop-loss triggered for {symbol}: close={close}, sl_price={sl_price}")
                            return f"SELL:{rule.get('id', 'fixed')}"
                    elif rule['type'] == 'trailing':
                        sl_value = float(rule['value'].strip('%')) / 100
                        position['highest_price'] = max(position['highest_price'], close)
                        sl_price = position['highest_price'] * (1 - sl_value)
                        if close <= sl_price:
                            logger.info(f"Trailing stop-loss triggered for {symbol}: close={close}, sl_price={sl_price}")
                            return f"SELL:{rule.get('id', 'trailing')}"
            else:
                if self.stop_loss['type'] == 'fixed':
                    sl_value = float(self.stop_loss['value'].strip('%')) / 100
                    sl_price = entry_price * (1 - sl_value)
                    if close <= sl_price:
                        logger.info(f"Stop-loss triggered for {symbol}: close={close}, sl_price={sl_price}")
                        return "SELL"
                elif self.stop_loss['type'] == 'trailing':
                    sl_value = float(self.stop_loss['value'].strip('%')) / 100
                    position['highest_price'] = max(position['highest_price'], close)
                    sl_price = position['highest_price'] * (1 - sl_value)
                    if close <= sl_price:
                        logger.info(f"Trailing stop-loss triggered for {symbol}: close={close}, sl_price={sl_price}")
                        return "SELL"
            return None
        except Exception as e:
            logger.error(f"Error evaluating stop-loss for {symbol}: {e}", exc_info=True)
            return None

    async def _evaluate_target(self, symbol: str, close: float, position: Dict) -> Optional[str]:
        """Evaluate target conditions."""
        try:
            if not self.target:
                return None
            entry_price = position['entry_price']
            if self.target['type'] == 'multi':
                for rule in self.target.get('rules', []):
                    if rule['type'] == 'fixed':
                        target_value = float(rule['value'].strip('%')) / 100
                        target_price = entry_price * (1 + target_value)
                        if close >= target_price:
                            logger.info(f"Target triggered for {symbol}: close={close}, target_price={target_price}")
                            if rule.get('partial_exit'):
                                partial_exit = rule['partial_exit'].strip('%')
                                return f"PARTIAL_SELL:{partial_exit}:{rule.get('id', 'fixed')}"
                            return f"SELL:{rule.get('id', 'fixed')}"
                    elif rule['type'] == 'trailing':
                        target_value = float(rule['value'].strip('%')) / 100
                        position['lowest_price'] = min(position['lowest_price'], close)
                        target_price = position['lowest_price'] * (1 + target_value)
                        if close >= target_price:
                            logger.info(f"Trailing target triggered for {symbol}: close={close}, target_price={target_price}")
                            if rule.get('partial_exit'):
                                partial_exit = rule['partial_exit'].strip('%')
                                return f"PARTIAL_SELL:{partial_exit}:{rule.get('id', 'trailing')}"
                            return f"SELL:{rule.get('id', 'trailing')}"
            else:
                if self.target['type'] == 'fixed':
                    target_value = float(self.target['value'].strip('%')) / 100
                    target_price = entry_price * (1 + target_value)
                    if close >= target_price:
                        logger.info(f"Target triggered for {symbol}: close={close}, target_price={target_price}")
                        return "SELL"
                elif self.target['type'] == 'trailing':
                    target_value = float(self.target['value'].strip('%')) / 100
                    position['lowest_price'] = min(position['lowest_price'], close)
                    target_price = position['lowest_price'] * (1 + target_value)
                    if close >= target_price:
                        logger.info(f"Trailing target triggered for {symbol}: close={close}, target_price={target_price}")
                        return "SELL"
            return None
        except Exception as e:
            logger.error(f"Error evaluating target for {symbol}: {e}", exc_info=True)
            return None

    def _evaluate_breakeven(self, symbol: str, close: float, position: Dict) -> bool:
        """Evaluate breakeven trigger."""
        try:
            if not self.trade_management or 'breakeven' not in self.trade_management:
                return False
            if position['breakeven_triggered']:
                return False
            trigger = self.trade_management['breakeven']['trigger'] / 100
            breakeven_price = position['entry_price'] * (1 + trigger)
            if close >= breakeven_price:
                logger.info(f"Breakeven triggered for {symbol}: close={close}, breakeven_price={breakeven_price}")
                return True
            return False
        except Exception as e:
            logger.error(f"Error evaluating breakeven for {symbol}: {e}", exc_info=True)
            return False

    async def _log_audit_trail(self, symbol: str, ohlcv: Dict, indicators: Dict, signal: str, reason: str):
        """Log audit trail for signal generation."""
        try:
            audit_data = {
                'symbol': symbol,
                'strategy': self.name,
                'signal': signal,
                'timestamp': ohlcv['timestamp'],
                'price': ohlcv['close'],
                'reason': reason,
                'ohlcv': ohlcv,
                'indicators': indicators
            }
            await self.redis_client.publish(f"signals:audit:{self.name}", json.dumps(audit_data))
            await self.database.save_audit_trail(audit_data)
            logger.debug(f"Logged audit trail for {symbol}: {signal} ({reason})")
        except Exception as e:
            logger.error(f"Error logging audit trail for {symbol}: {e}", exc_info=True)

    async def stop(self):
        """Close RuleEngine connections."""
        logger.info(f"Closing RuleEngine for {self.name}")
        try:
            await self.redis_client.close()
            await self.database.close()
            logger.info(f"RuleEngine for {self.name} closed successfully")
        except Exception as e:
            logger.error(f"Error closing RuleEngine for {self.name}: {e}", exc_info=True)
            raise