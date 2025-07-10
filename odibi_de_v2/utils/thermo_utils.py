from iapws import IAPWS97
from psychrolib import SetUnitSystem, GetHumRatioFromRelHum, IP
from math import exp
from typing import Optional


def compute_steam_properties(row, input_params: dict, output_properties: list, prefix: str = "") -> dict:
    """
    Computes steam properties based on the IAPWS97 formulation for a given row of data.

    This function calculates specific steam properties such as enthalpy, entropy, and heat capacity
    at constant pressure, using the IAPWS97 standard. The properties are computed based on input parameters
    provided for each row of a dataset, typically represented as a Pandas Series.

    Args:
        row (pd.Series): The input row from a Pandas DataFrame, containing the necessary data.
        input_params (dict): A dictionary where keys are the names of the parameters required by the IAPWS97
            standard (e.g., "P" for pressure, "T" for temperature). The values can be either constants, column
            names from the `row`, or functions that take `row` as an argument and return a value.
        output_properties (list of str): A list of property names (as strings) that are to be calculated and
            returned (e.g., ["h", "s", "cp"] for enthalpy, entropy, and specific heat at constant pressure).
        prefix (str, optional): A prefix string that will be prepended to each property name in the output
            dictionary to create unique column names. Defaults to an empty string.

    Returns:
        dict: A dictionary with keys formatted as "{prefix}_{property}" where each key corresponds to a
        property name from `output_properties`, and the values are the calculated properties based on the IAPWS97 standard.

    Raises:
        Exception: If an error occurs during the calculation of properties, such as missing data or
        invalid inputs, the function will catch the exception and return None for each requested property.

    Example:
        >>> row = pd.Series({'P': 101325, 'T': 300})
        >>> input_params = {'P': 'P', 'T': 'T'}
        >>> output_properties = ['h', 's', 'cp']
        >>> compute_steam_properties(row, input_params, output_properties)
        {'h': 115.5, 's': 0.5, 'cp': 4.2}
    """
    try:
        # Dynamically resolve arguments
        resolved_args = {
            k: v(row) if callable(v) else row[v] if isinstance(v, str) else v
            for k, v in input_params.items()
        }

        state = IAPWS97(**resolved_args)

        return {
            f"{prefix}_{prop}": getattr(state, prop, None)
            for prop in output_properties
        }

    except Exception as e:
        return {f"{prefix}_{prop}": None for prop in output_properties}


def compute_humidity_ratio(
    Tdb: float,
    RH: float,
    Elev_ft: Optional[float] = None,
    pressure: Optional[float] = None
) -> Optional[float]:
    """
    Compute the humidity ratio (lb water vapor / lb dry air) given dry bulb temperature,
    relative humidity, and either elevation (ft) or pressure (psia) using psychrolib in IP units.

    This function is used for psychrometric analysis in imperial units. If pressure is not provided,
    it will be estimated from elevation using the barometric formula.

    Args:
        Tdb (float): Dry bulb temperature in degrees Fahrenheit.
        RH (float): Relative humidity as a percentage (e.g., 65.0 for 65% RH).
        Elev_ft (float, optional): Elevation in feet, used to estimate pressure if `pressure` is not provided.
        pressure (float, optional): Atmospheric pressure in psia. Takes precedence over Elev_ft if provided.

    Returns:
        float: Humidity ratio (lb water vapor / lb dry air), or None if an error occurs.

    Example:
        >>> import pandas as pd
        >>> from odibi_de_v2.utils import compute_humidity_ratio
        >>> df = pd.DataFrame({
        ...     "Tdb": [80.0, 75.0, 90.0],
        ...     "RH": [60.0, 45.0, 70.0],
        ...     "Elev_ft": [875, 500, 1000]
        ... })
        >>> df["humidity_ratio"] = df.apply(
        ...     lambda row: compute_humidity_ratio(row["Tdb"], row["RH"], Elev_ft=row["Elev_ft"]),
        ...     axis=1
        ... )
        >>> print(df)
    """
    try:
        SetUnitSystem(IP)
        Tdb = float(Tdb)
        RH_frac = float(RH / 100)

        if pressure is not None:
            pressure = float(pressure)
        elif Elev_ft is not None:
            Elev_ft = float(Elev_ft)
            pressure = 14.696 * exp(-0.0000366 * Elev_ft)
        else:
            raise ValueError("Either `pressure` or `Elev_ft` must be provided.")
        return GetHumRatioFromRelHum(
            Tdb, RH_frac, pressure
            )

    except Exception as e:
        print(f"Error in compute_humidity_ratio: {e}")
        return None



