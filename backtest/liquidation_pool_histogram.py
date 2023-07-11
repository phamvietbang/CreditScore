from datetime import datetime

import pandas as pd
from matplotlib import pyplot as plt
if __name__ == "__main__":
    data = pd.read_csv("")
    data = data[1:31]
    tmp = 0
    total = 0
    result = {"Quarter": [], "Value": []}
    for i in range(1, 31):
        total += data['total'][i]
        tmp += 1
        if tmp == 3:
            date = data["timestamps"][i].split("-")
            str_ = ""
            if date[1] == "01":
                str_ = f"Q4-{date[0]}"
            if date[1] == "04":
                str_ = f"Q1-{date[0]}"
            if date[1] == "07":
                str_ = f"Q2-{date[0]}"
            if date[1] == "10":
                str_ = f"Q3-{date[0]}"
            result["Value"].append(total)
            result["Quarter"].append(str_)
            tmp = 0
            total = 0

    result = pd.DataFrame(result)
    result.to_csv("bsc_LM.csv")
    ax = result.plot.bar(x="Quarter", y="Value")
    ax.set_title("Liquidation Amount")
    ax.set_ylabel("Amount ($)")
    ax.set_xlabel("Time")
    ax.get_legend().remove()
    plt.show()

