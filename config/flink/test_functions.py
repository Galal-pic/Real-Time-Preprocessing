import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _check_numbers(operator: str, value: int, transaction_float: float) -> bool:
    """Evaluate a numeric condition on the transaction amount."""
    try:
        if operator == ">":
            return transaction_float > value
        elif operator == "<":
            return transaction_float < value
        elif operator == "==":
            return transaction_float == value
        elif operator == ">=":
            return transaction_float >= value
        elif operator == "<=":
            return transaction_float <= value
        else:
            return False
    except Exception as e:
        logger.error(f"Error in _check_numbers: {e}")
        return False


def _check_string(operator: str, value: str, transaction: str) -> bool:
    """Evaluate a string condition."""
    return value == transaction if operator == "==" else False


def _check_bool(operator: str, value: bool, transaction: bool) -> bool:
    """Evaluate a boolean condition."""
    return transaction == value if operator == "==" else False


def _check_conditions(
    transaction: Dict[str, Any],
    conditions: list[Dict[str, Any]],
    customer_profile_conditions: list[Dict[str, Any]],
    tragged_conditions: str,
    source_topic: str,
) -> bool:
    """Check if the transaction meets the conditions."""
    try:
        if tragged_conditions != source_topic:
            return False

        for condition in conditions:
            for key, rule in condition.items():
                if isinstance(rule["value"], int):
                    if not _check_numbers(
                        rule["operator"], rule["value"], float(transaction[key][1:])
                    ):
                        return False
                elif isinstance(rule["value"], str):
                    if not _check_string(
                        rule["operator"], rule["value"], transaction[key]
                    ):
                        return False
                elif isinstance(rule["value"], bool):
                    if not _check_bool(
                        rule["operator"], rule["value"], bool(transaction[key])
                    ):
                        return False

        for CP_condation in customer_profile_conditions:
            for key, rule in CP_condation.items():
                if isinstance(rule["value"], str):
                    if not _check_string(
                        rule["operator"], rule["value"], transaction[key]
                    ):
                        return False
                elif isinstance(rule["value"], int):
                    if not _check_numbers(
                        rule["operator"], rule["value"], transaction[key]
                    ):
                        return False
                elif isinstance(rule["value"], bool):
                    if not _check_bool(
                        rule["operator"], rule["value"], transaction[key]
                    ):
                        return False

        return True
    except Exception as e:
        logger.error(f"Error in _check_conditions: {e}")
        return False
