import json
import logging
from pyflink.datastream.functions import MapFunction
import pandas as pd
from pathlib import Path
from typing import List, Tuple
from .Enrich import _enrich_transaction
from .test_functions import _check_conditions

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BusinessRulesParser(MapFunction):
    def __init__(
        self,
        source_topic: str,
        database: pd.DataFrame,
        column: str,
        rules_file_path: str = "dataset/Rules.json",
    ):
        self.rules_file_path = Path(rules_file_path)
        self.business_rules = self._load_business_rules()
        self.source_topic = source_topic
        self.database = database
        self.column = column

    def _load_business_rules(self) -> List[dict]:
        """Load business rules from the JSON file."""
        try:
            with open(self.rules_file_path, "r") as file:
                json_data = json.load(file)
                return json_data.get("businessRules", [])
        except Exception as e:
            logger.error(f"Error loading business rules: {e}")
            return []

    def map(self, value: str) -> List[Tuple[str, str]]:
        """Process incoming transaction data against business rules."""
        try:
            value = value.strip()
            if not value:
                return [("Error", "Empty input")]

            test_cases = json.loads(value)
            if isinstance(test_cases, dict):
                test_cases = [test_cases]
            elif not isinstance(test_cases, list):
                return [
                    ("Error", "Invalid input format - expected JSON object or array")
                ]

            action_messages = []
            for test_case in test_cases:
                test_case = _enrich_transaction(test_case, self.database, self.column)
                for business_rule in self.business_rules:
                    conditions = business_rule.get("conditions", [])
                    customer_profile_conditions = business_rule.get(
                        "customerProfile", []
                    )
                    tragged_conditions = business_rule.get("triggerEvent")

                    if _check_conditions(
                        test_case,
                        conditions,
                        customer_profile_conditions,
                        tragged_conditions,
                        self.source_topic,
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
            logger.error(f"Unexpected error: {e}")
            return [("Error", f"Unexpected error: {str(e)}")]
