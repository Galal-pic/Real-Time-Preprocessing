import json
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types
from pyflink.datastream.functions import MapFunction

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)


class BusinessRulesParser(MapFunction):
    def map(self, value):
        try:
            action_messages = []
            # open rules
            with open("dataset\Transactions&Rules.json", "r") as file:
                json_data = json.load(file)
                business_rules = json_data.get("businessRules", [])

                # loop on condations
            for business_rule in business_rules:
                conditions = business_rule.get("conditions", {})
                transaction_type = conditions.get("transactionType", "")
                transaction_amount = conditions.get("transactionAmount", {})
                transaction_operator = transaction_amount.get("operator", "")
                transaction_value = transaction_amount.get("value", 0)
                customer_profile = conditions.get("customerProfile", {})
                customer_segment = customer_profile.get("segment", "")
                has_installment_card = customer_profile.get("hasInstallmentCard", False)

                # Evaluate the conditions for each test case
                test_cases = json.loads(value.strip())

                for test_case in test_cases:
                    condition_met = True
                    # Check if transaction type matches
                    if test_case["transactionType"] != transaction_type:
                        condition_met = False

                    # Check if transaction amount meets the operator condition
                    if (
                        transaction_operator == "greaterThan"
                        and test_case["transactionAmount"] <= transaction_value
                    ):
                        condition_met = False

                    # Check customer profile conditions
                    if test_case["customerProfile"]["segment"] != customer_segment:
                        condition_met = False
                    if (
                        test_case["customerProfile"]["hasInstallmentCard"]
                        != has_installment_card
                    ):
                        condition_met = False

                    # If all conditions are met, extract the action message
                    if condition_met:
                        action = business_rule.get("action", {})
                        action_message = action.get("message", "")

                        # Add the action message to the list of results
                        action_messages.append(action_message)

            # Return only the action messages
            return action_messages
        except Exception as e:
            print(f"Error processing JSON: {e}")
            return [f"Error: {e}"]


def main():
    # Directly read the entire file contents
    file_path = r"D:\SIA\Kafka-Flink\dataset\Transactions.json"

    with open(file_path, "r", encoding="utf-8") as file:
        file_contents = file.read()

    # Create a stream from the file contents
    data_stream = env.from_collection(["galal", "ewida"])

    # Map and process the business rules
    parsed_stream = data_stream.map(
        BusinessRulesParser(), output_type=Types.LIST(Types.STRING())
    )

    # Collect the results to a list
    result = parsed_stream.execute_and_collect()

    # Print the results
    for action_message in result:
        print("--------------------------------" * 3)
        print(f"Action Message: {action_message}")


if __name__ == "__main__":
    main()
