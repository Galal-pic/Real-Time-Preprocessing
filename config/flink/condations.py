def _check_numbers(self, operator, value, transaction):
    """Evaluate a numeric condition on the transaction amount."""
    try:
        transaction_float = float(transaction["Amount"][1:])

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


def _check_category(self, operator, value, transaction, condation_name):
    """Evaluate a category condition on the transaction MCC code."""
    if condation_name == "category":
        if operator == "==":
            return transaction["category"] == value
    elif condation_name == "Segment":
        if operator == "==":
            return transaction["Segment"] == value


def _check_status(self, operator, value, transaction):
    """Evaluate a category condition on the transaction MCC code."""
    if operator == "==":
        return bool(transaction["hasInstallmentCard"]) == value
