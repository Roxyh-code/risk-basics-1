import pandas as pd

FILE_PATH = r"Assignment 1.1 -  Input Data 2024.xlsx"
OUTPUT_FILE = "assignment_1_1_results.xlsx"

HYP0 = 100.0
ASSET_COLS = ["Equity", "Rates", "Commodity", "FX", "Credit"]


def load_data(file_path):
    # 读三个 sheet
    bom = pd.read_excel(file_path, sheet_name="BOM AUM")[["Date", "BOM AUM"]]
    alloc = pd.read_excel(file_path, sheet_name="Allocation")[["Date", "Allocation"]]
    pnl = pd.read_excel(file_path, sheet_name="PnL by Asset Class")[["Date", "Asset Class", "Daily PnL"]]

    # 简单整理列名和日期
    bom.columns = ["Date", "BOM_AUM"]
    alloc.columns = ["Date", "Flow"]

    bom["Date"] = pd.to_datetime(bom["Date"]).dt.normalize()
    alloc["Date"] = pd.to_datetime(alloc["Date"]).dt.normalize()
    pnl["Date"] = pd.to_datetime(pnl["Date"]).dt.normalize()

    # 同一天 flow 合并
    alloc = alloc.groupby("Date", as_index=False)["Flow"].sum()

    # PnL 转宽表
    pnl = pnl[pnl["Asset Class"].isin(ASSET_COLS)]
    pnl = pnl.pivot_table(
        index="Date",
        columns="Asset Class",
        values="Daily PnL",
        aggfunc="sum"
    ).reset_index()

    # 保证资产列都在
    for col in ASSET_COLS:
        if col not in pnl.columns:
            pnl[col] = 0.0

    pnl = pnl[["Date"] + ASSET_COLS].sort_values("Date").reset_index(drop=True)

    return bom, alloc, pnl


def build_daily(bom, alloc, pnl):
    daily = pnl.merge(alloc, on="Date", how="left").merge(bom, on="Date", how="left")
    daily["Flow"] = daily["Flow"].fillna(0.0)

    daily["Month"] = daily["Date"].dt.to_period("M").astype(str)
    daily["Total_PnL"] = daily[ASSET_COLS].sum(axis=1)

    # 找到每个月第一天
    daily["is_month_start"] = daily.groupby("Month").cumcount() == 0

    # 把月初 BOM_AUM 填到整个月
    daily["Month_BOM"] = daily["BOM_AUM"].where(daily["is_month_start"])
    daily["Month_BOM"] = daily.groupby("Month")["Month_BOM"].transform("first")

    # BOD_AUM = 月初BOM + 本月之前累计 (前一日PnL + 前一日Flow)
    prev_change = (daily["Total_PnL"] + daily["Flow"]).shift(1).fillna(0.0)
    prev_change[daily["is_month_start"]] = 0.0
    daily["BOD_AUM"] = daily["Month_BOM"] + prev_change.groupby(daily["Month"]).cumsum()

    # 日收益率
    daily["R_port"] = daily["Total_PnL"] / daily["BOD_AUM"]

    for col in ASSET_COLS:
        daily[f"R_{col}"] = daily[col] / daily["BOD_AUM"]

    daily_output = daily[
        ["Date", "Month", "Flow"] + ASSET_COLS +
        ["Total_PnL", "BOD_AUM", "R_port"] +
        [f"R_{col}" for col in ASSET_COLS]
    ].copy()

    return daily_output


def build_monthly(daily):
    df = daily.copy()
    df["Gross_Port"] = 1 + df["R_port"]

    # 月度组合收益
    monthly_return = df.groupby("Month")["Gross_Port"].prod() - 1

    # Hypothetical AUM path
    cum_gross = df.groupby("Month")["Gross_Port"].cumprod()
    aum_mult = cum_gross.groupby(df["Month"]).shift(1).fillna(1.0)
    df["Hyp_AUM_Start"] = HYP0 * aum_mult

    # 月度 attribution
    hyp_pnl = pd.DataFrame()
    for col in ASSET_COLS:
        hyp_pnl[f"{col}_Contrib"] = df[f"R_{col}"] * df["Hyp_AUM_Start"]

    hyp_pnl["Month"] = df["Month"]
    monthly_contrib = hyp_pnl.groupby("Month").sum() / HYP0

    monthly = pd.DataFrame({
        "Month": monthly_return.index,
        "Portfolio_Return": monthly_return.values,
        "Month_Start": df.groupby("Month")["Date"].min().values,
        "Month_End": df.groupby("Month")["Date"].max().values,
    })

    monthly = monthly.merge(monthly_contrib.reset_index(), on="Month", how="left")
    monthly["Attrib_Sum"] = monthly[[f"{col}_Contrib" for col in ASSET_COLS]].sum(axis=1)
    monthly["Attrib_Error"] = (monthly["Attrib_Sum"] - monthly["Portfolio_Return"]).abs()

    return monthly


def build_yearly(monthly):
    df = monthly.copy()
    df["Year"] = pd.to_datetime(df["Month"] + "-01").dt.year
    df["Gross_Port"] = 1 + df["Portfolio_Return"]

    # 年度组合收益
    yearly_return = df.groupby("Year")["Gross_Port"].prod() - 1

    # 年度 hypothetical path
    cum_gross = df.groupby("Year")["Gross_Port"].cumprod()
    aum_mult = cum_gross.groupby(df["Year"]).shift(1).fillna(1.0)
    df["Hyp_AUM_Start"] = HYP0 * aum_mult

    # 年度 attribution
    yearly_contrib_data = {}
    for col in ASSET_COLS:
        yearly_contrib_data[f"{col}_Contrib"] = (
            df[f"{col}_Contrib"] * df["Hyp_AUM_Start"]
        ).groupby(df["Year"]).sum() / HYP0

    yearly = pd.DataFrame({
        "Year": yearly_return.index,
        "Portfolio_Return": yearly_return.values,
        "Year_Start": df.groupby("Year")["Month_Start"].min().values,
        "Year_End": df.groupby("Year")["Month_End"].max().values,
    })

    for col in ASSET_COLS:
        yearly[f"{col}_Contrib"] = yearly["Year"].map(yearly_contrib_data[f"{col}_Contrib"])

    yearly["Attrib_Sum"] = yearly[[f"{col}_Contrib" for col in ASSET_COLS]].sum(axis=1)
    yearly["Attrib_Error"] = (yearly["Attrib_Sum"] - yearly["Portfolio_Return"]).abs()

    return yearly


def save_results(daily, monthly, yearly, output_file):
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        daily.to_excel(writer, sheet_name="Daily", index=False)
        monthly.to_excel(writer, sheet_name="Monthly", index=False)
        yearly.to_excel(writer, sheet_name="Yearly", index=False)


def main():
    bom, alloc, pnl = load_data(FILE_PATH)
    daily = build_daily(bom, alloc, pnl)
    monthly = build_monthly(daily)
    yearly = build_yearly(monthly)

    save_results(daily, monthly, yearly, OUTPUT_FILE)

    print("Done.")
    print(f"Daily records: {len(daily)}")
    print(f"Monthly records: {len(monthly)}")
    print(f"Yearly records: {len(yearly)}")
    print(f"Saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()