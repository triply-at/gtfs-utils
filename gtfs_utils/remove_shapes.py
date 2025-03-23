from pathlib import Path

import numpy as np


def remove_shapes(df_dict, output):
    output_dir = Path(output)
    df_dict["trips"] = df_dict["trips"].assign(shape_id=np.nan)
    df_dict["trips"].to_csv(output_dir / "trips.txt", single_file=True, index=False)
