import logging


def analyze_route_type(df_dict):
    dic = df_dict["routes"]["route_type"].unique().compute().sort_values()
    logging.info("route_types: " + " ".join(map(lambda x: str(x), dic)))
