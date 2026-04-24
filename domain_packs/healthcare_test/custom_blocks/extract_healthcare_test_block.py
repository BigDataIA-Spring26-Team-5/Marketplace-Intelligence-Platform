import logging
import pandas as pd
from src.blocks.base import Block

logger = logging.getLogger(__name__)


class ExtractHealthcareTestBlock(Block):
    name = "healthcare_test__extract_patient_last_name"
    domain = "healthcare_test"
    description = "Extract patient last name from patient name field"
    inputs = ["patient_name"]
    outputs = ["patient_last_name"]

    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        if "patient_name" not in df.columns:
            df["patient_last_name"] = pd.NA
            return df
        df["patient_last_name"] = df["patient_name"].apply(self._extract_last_name)
        return df

    def _extract_last_name(self, name: object) -> str | None:
        if not isinstance(name, str) or not name.strip():
            return None
        parts = name.strip().split()
        if len(parts) == 0:
            return None
        return parts[-1]