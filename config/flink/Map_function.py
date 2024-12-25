import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction
from pyflink.common.typeinfo import Types
import pandas as pd

# Load MCC codes once at the global scope to avoid reloading in every operation
mcc_df = pd.read_csv("dataset/mcc_codes.csv")


class BusinessRulesParser(MapFunction):
    def __init__(self, rules_file_path="dataset/Transactions&Rules.json"):
        self.rules_file_path = rules_file_path
        self.business_rules = self._load_business_rules()

    def _load_business_rules(self):
        """Load business rules from the JSON file."""
        try:
            with open(self.rules_file_path, "r") as file:

                json_data = json.load(file)
                return json_data.get("businessRules", [])
        except Exception as e:
            print(f"Error loading business rules: {e}")
            return []

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

    def _check_category(self, operator, value, transaction):
        """Evaluate a category condition on the transaction MCC code."""
        if operator == "==":
            return transaction["Category"] == value

    def map(self, value):
        """Process incoming transaction data against business rules."""
        try:
            value = value.strip()
            if not value:
                return "Error: Empty input"

            # Parse JSON input
            try:
                test_cases = json.loads(value)
            except json.JSONDecodeError as e:
                print(f"JSON parsing error: {e}")
                return f"Error: Invalid JSON format - {str(e)}"

            # Ensure test_cases is a list
            if isinstance(test_cases, dict):
                test_cases = [test_cases]
            elif not isinstance(test_cases, list):
                return "Error: Invalid input format - expected JSON object or array"

            action_messages = []
            for test_case in test_cases:
                test_case = self._enrich_transaction(test_case)

                for business_rule in self.business_rules:
                    conditions = business_rule.get("conditions", [])
                    customer_profile_conditions = business_rule.get(
                        "customerProfile", []
                    )

                    if self._check_conditions(
                        test_case, conditions, customer_profile_conditions
                    ):
                        action = business_rule.get("action", {})
                        action_message = action.get("message", "")
                        action_messages.append(
                            (str(business_rule.get("triggerEvent")), action_message)
                        )

            return (
                action_messages
                if action_messages
                else [("None", "No matching rules found")]
            )

        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            return f"Error: {str(e)}"

    def _enrich_transaction(self, transaction):
        """Enrich the transaction with MCC data."""
        try:
            transaction_df = pd.DataFrame([transaction])
            enriched_transaction = pd.merge(
                transaction_df, mcc_df, on="MCC", how="left"
            ).to_dict(orient="records")[0]
            return enriched_transaction
        except Exception as e:
            print(f"Error enriching transaction: {e}")
            return transaction

    def _check_conditions(self, transaction, conditions, customer_profile_conditions):
        """Check if the transaction meets the conditions."""
        try:
            # Evaluate conditions
            for condition in conditions:
                if condition.get("Amount", "") != "":
                    # print(
                    #     f"Amount = operator - > {condition['Amount']['operator']} , value - > {condition['Amount']['value']}"
                    # )

                    if not self._check_numbers(
                        condition["Amount"]["operator"],
                        condition["Amount"]["value"],
                        transaction,
                    ):
                        return False
                else:
                    # print(
                    #     f"category = operator - > {condition['Category']['operator']} , value - > {condition['Category']['value']}"
                    # )
                    if not self._check_category(
                        condition["Category"]["operator"],
                        condition["Category"]["value"],
                        transaction,
                    ):
                        return False

            # Evaluate customer profile conditions
            # customer_profile = transaction.get("customerProfile", {})
            # for key, value in customer_profile_conditions.items():
            #     if customer_profile.get(key) != value:
            #         return False

            return True
        except Exception as e:
            print(f"Error in _check_conditions: {e}")
            return False
