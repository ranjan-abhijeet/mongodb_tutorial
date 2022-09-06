def get_operator(symbol: str) -> str:
    """
    The class contains static methods which return comparision operator for 
    MongoDB based no-sql queries.

    Name                    Description

    $eq             Matches values that are equal to a specified value.
    $gt             Matches values that are greater than a specified value.
    $gte            Matches values that are greater than or equal to a specified value.
    $in             Matches any of the values specified in an array.
    $lt             Matches values that are less than a specified value.
    $lte            Matches values that are less than or equal to a specified value.
    $ne             Matches all values that are not equal to a specified value.
    $nin            Matches none of the values specified in an array.
    """
    if symbol == "and":
        return "$and"
    elif symbol == "or":
        return "$or"
    elif symbol == "==":
        return "$eq"
    elif symbol == ">":
        return "$gt"
    elif symbol == ">=":
        return "$gte"
    elif symbol == "<":
        return "$lt"
    elif symbol == "<=":
        return "$lte"
    elif symbol == "!=":
        return "$ne"
    elif symbol == "in":
        return "$in"
    elif symbol == "nin":
        return "$nin"

