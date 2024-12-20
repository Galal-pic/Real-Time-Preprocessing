import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction
from pyflink.common.typeinfo import Types


class BusinessRulesParser(MapFunction):
    def __init__(self, rules_file_path="dataset/Transactions&Rules.json"):
        self.rules_file_path = rules_file_path
        self.business_rules = self._load_business_rules()

    def _load_business_rules(self):
        try:
            with open(self.rules_file_path, "r") as file:
                json_data = json.load(file)
                return json_data.get("businessRules", [])
        except Exception as e:
            print(f"Error loading business rules: {e}")
            return []

    def map(self, value):
        try:
            # Try to parse the string as JSON
            try:
                # Strip any whitespace and check if the string is empty
                value = value.strip()
                if not value:
                    return "Error: Empty input"

                test_cases = json.loads(value)

            except json.JSONDecodeError as e:
                print(f"JSON parsing error: {e}")
                return f"Error: Invalid JSON format - {str(e)}"

            # Handle single object vs array
            if isinstance(test_cases, dict):
                test_cases = [test_cases]
            elif not isinstance(test_cases, list):
                return f"Error: Invalid input format - expected JSON object or array"

            action_messages = []
            for test_case in test_cases:

                if not isinstance(test_case, dict):
                    continue

                # Process each business rule
                for business_rule in self.business_rules:
                    conditions = business_rule.get("conditions", {})
                    transaction_type = conditions.get("transactionType", "")
                    transaction_amount = conditions.get("transactionAmount", {})
                    transaction_operator = transaction_amount.get("operator", "")
                    transaction_value = transaction_amount.get("value", 0)
                    customer_profile = conditions.get("customerProfile", {})
                    customer_segment = customer_profile.get("segment", "")
                    has_installment_card = customer_profile.get(
                        "hasInstallmentCard", False
                    )

                    condition_met = self._check_conditions(
                        test_case,
                        transaction_type,
                        transaction_operator,
                        transaction_value,
                        customer_segment,
                        has_installment_card,
                    )

                    if condition_met:
                        action = business_rule.get("action", {})
                        action_message = action.get("message", "")
                        action_messages.append(
                            (str(transaction_type), str(action_message))
                        )

            return (
                action_messages
                if action_messages
                else [("None", "No matching rules found")]
            )

        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            import traceback

            traceback.print_exc()
            return f"Error: {str(e)}"

    def _check_conditions(
        self,
        test_case,
        transaction_type,
        transaction_operator,
        transaction_value,
        customer_segment,
        has_installment_card,
    ):
        try:
            # Check transaction type
            test_trans_type = test_case.get("transactionType")
            if test_trans_type != transaction_type:
                return False

            # Check transaction amount
            test_amount = test_case.get("transactionAmount", 0)
            if (
                transaction_operator == "greaterThan"
                and test_amount <= transaction_value
            ):
                return False

            # Check customer profile
            customer_profile = test_case.get("customerProfile", {})
            if (
                customer_profile.get("segment") != customer_segment
                or customer_profile.get("hasInstallmentCard") != has_installment_card
            ):
                return False

            return True

        except Exception as e:
            print(f"Error in _check_conditions: {str(e)}")
            return False


def split_and_process_data(value):

    parts = value.split(",")

    return parts
