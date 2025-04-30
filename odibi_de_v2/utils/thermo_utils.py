from iapws import IAPWS97


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
