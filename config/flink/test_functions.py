import json


def _check_numbers(operator, value, transaction_float):
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
        print(f"Error in _check_numbers: {e}")
        return False


def _check_string(operator, value, transaction):
    """Evaluate a string condition."""
    if operator == "==":
        return value == transaction


def _check_bool(operator, value, transaction):
    """Evaluate a boolean condition."""
    if operator == "==":
        return transaction == value


def _check_conditions(
    transaction,
    conditions,
    customer_profile_conditions,
    tragged_conditions,
    source_topic,
):
    """Check if the transaction meets the conditions."""
    try:
        if tragged_conditions != source_topic:
            return False
        # Evaluate conditions
        for condition in conditions:
            for key, rule in condition.items():
                # IF integer value
                if isinstance(rule["value"], int):
                    if not _check_numbers(
                        rule["operator"],
                        rule["value"],
                        float(transaction[key][1:]),
                    ):
                        return False
                # Else string
                elif isinstance(rule["value"], str):
                    if not _check_string(
                        rule["operator"],
                        rule["value"],
                        transaction[key],
                    ):
                        return False
                else:
                    if isinstance(rule["value"], bool):
                        if not _check_bool(
                            rule["operator"],
                            rule["value"],
                            bool(transaction[key]),
                        ):
                            return False

        for CP_condation in customer_profile_conditions:
            for key, rule in CP_condation.items():
                # print(
                #     f"{key} = operator - > {rule['operator']} , value - > {rule['value']}"
                # )
                if isinstance(rule["value"], str):
                    if not _check_string(
                        rule["operator"],
                        rule["value"],
                        transaction[key],
                    ):
                        return False
                elif isinstance(rule["value"], int):
                    if not _check_numbers(
                        rule["operator"],
                        rule["value"],
                        transaction[key],
                    ):
                        return False
                else:
                    if isinstance(rule["value"], bool):
                        if not _check_bool(
                            rule["operator"],
                            rule["value"],
                            transaction[key],
                        ):
                            return False

        return True
    except Exception as e:
        print(f"Error in _check_conditions: {e}")
        return False
