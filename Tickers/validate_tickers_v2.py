"""
validate_tickers_v2.py
----------------------
Version corrigée : utilise yf.Ticker().history() au lieu de yf.download()
plus robuste pour les tickers internationaux.
"""

import yfinance as yf
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
os.chdir(r"C:\Users\Tom\Desktop\Tickers")

TICKERS = {

    "US": [
        # Technologie
        "AAPL","MSFT","NVDA","GOOGL","META","AVGO","ORCL","CRM","AMD",
        "INTC","QCOM","TXN","MU","AMAT","LRCX","KLAC","ADI","MRVL",
        "SNPS","CDNS",
        # Finance
        "BRK-B","JPM","BAC","WFC","GS","MS","C","AXP","BLK","SCHW",
        "USB","PNC","TFC","COF","MET","PRU","AIG","AFL","ALL","CB",
        # Santé
        "JNJ","UNH","PFE","ABBV","LLY","MRK","TMO","ABT","DHR","BMY",
        "AMGN","GILD","ISRG","SYK","BSX","MDT","ZTS","REGN","VRTX","BIIB",
        # Conso discrétionnaire
        "AMZN","TSLA","HD","MCD","NKE","SBUX","LOW","TGT","BKNG","MAR",
        "HLT","YUM","DRI","CMG","ROST","TJX","BBY","ABNB",
        # Conso de base
        "PG","KO","PEP","WMT","COST","PM","MO","CL","KMB","CHD",
        "GIS","K","CPB","CAG","MKC","HRL","TSN","KR",
        # Énergie
        "XOM","CVX","COP","EOG","SLB","PSX","VLO","MPC","OXY","HAL","DVN","HES","APA",
        # Industrie
        "HON","UPS","RTX","LMT","GE","CAT","DE","MMM","EMR","ETN",
        "PH","ROK","CMI","ITW","DOV","FDX","NSC","UNP","CSX","WAB",
        # Matériaux
        "LIN","APD","SHW","ECL","NEM","FCX","NUE","STLD","ALB","CF",
        # Utilities
        "NEE","DUK","SO","D","AEP","EXC","XEL","SRE","PCG","ED",
        # REITs
        "PLD","AMT","EQIX","CCI","PSA","WELL","DLR","O","SPG","AVB",
        # Communication
        "NFLX","DIS","CMCSA","T","VZ","TMUS","CHTR",
        # Mid/Small + ADR
        "ENPH","FSLR","DKNG","MGM","LVS","ZM","DOCU","TWLO","BILL","HUBS",
        "TTWO","EA","DXCM","PODD","ALGN","LULU","ETSY",
        "BIDU","JD","PDD","BABA","NTES",
        "MELI","NU","STNE","GRAB","SEA",
    ],

    "Europe": [
        # France
        "MC.PA","TTE.PA","SAN.PA","OR.PA","BNP.PA",
        "AIR.PA","RI.PA","CS.PA","KER.PA","SGO.PA",
        "DG.PA","VIE.PA","GLE.PA","CAP.PA","DSY.PA",
        "PUB.PA","RNO.PA","STM.PA","HO.PA","ACA.PA",
        # Allemagne
        "SAP.DE","SIE.DE","ALV.DE","MBG.DE","BMW.DE",
        "BAYN.DE","BASF.DE","DTE.DE","MUV2.DE","ADS.DE",
        "VOW3.DE","DB1.DE","HEN3.DE","RWE.DE","BEI.DE",
        "HEI.DE","IFX.DE","FRE.DE","MTX.DE","CON.DE",
        # Royaume-Uni
        "SHEL.L","AZN.L","HSBA.L","ULVR.L","BP.L",
        "GSK.L","RIO.L","VOD.L","LLOY.L","BATS.L",
        "PRU.L","NG.L","REL.L","DGE.L","CPG.L",
        "IMB.L","ABF.L","WTB.L","MNDI.L","EXPN.L",
        # Pays-Bas
        "ASML.AS","INGA.AS","PHIA.AS","NN.AS","WKL.AS","HEIA.AS",
        # Belgique
        "AGS.BR","UCB.BR","SOLB.BR","AB.BR","ACKB.BR",
        # Suisse
        "NESN.SW","ROG.SW","NOVN.SW","ZURN.SW","UBSG.SW",
        "ABBN.SW","GIVN.SW","CFR.SW","LONN.SW","SIKA.SW",
        # Suède
        "VOLV-B.ST","ERIC-B.ST","SEB-A.ST","NDA-SE.ST","HEXA-B.ST",
        "ATCO-A.ST","SAND.ST","SKF-B.ST","ABB.ST","ALFA.ST",
        # Espagne / Italie
        "ITX.MC","SAN.MC","BBVA.MC","IBE.MC","REP.MC",
        "ENI.MI","ENEL.MI","ISP.MI","UCG.MI","STM.MI",
    ],

    "Asia": [
        # Japon
        "7203.T","6758.T","9432.T","9984.T","8306.T",
        "6501.T","6702.T","7267.T","4502.T","9433.T",
        "8316.T","6752.T","7741.T","4063.T","8035.T",
        "6594.T","4523.T","6367.T","2914.T","7974.T",
        "9983.T","3382.T","4568.T","6861.T","9022.T",
        "8766.T","4519.T","1925.T","5108.T","6981.T",
        # Australie
        "BHP.AX","CBA.AX","CSL.AX","NAB.AX","WBC.AX",
        "ANZ.AX","RIO.AX","WES.AX","WOW.AX","MQG.AX",
        "FMG.AX","STO.AX","TCL.AX","AMC.AX","COL.AX",
        "IAG.AX","QBE.AX","ALL.AX","WDS.AX","ORG.AX",
        # Hong Kong
        "9988.HK","0700.HK","0939.HK","1398.HK","2318.HK",
        "0005.HK","1299.HK","0941.HK","2628.HK","0388.HK",
        "0883.HK","2382.HK","0175.HK","0669.HK","1810.HK",
        # Singapour
        "D05.SI","O39.SI","U11.SI","Z74.SI","C6L.SI",
        # Taïwan
        "2330.TW","2317.TW","2454.TW","2308.TW","2882.TW",
        # Corée
        "005930.KS","000660.KS","035420.KS","051910.KS","006400.KS",
    ],

    "EM": [
        # Inde
        "RELIANCE.NS","TCS.NS","HDFCBANK.NS","INFY.NS","ICICIBANK.NS",
        "HINDUNILVR.NS","KOTAKBANK.NS","LT.NS","SBIN.NS","AXISBANK.NS",
        "WIPRO.NS","MARUTI.NS","BAJFINANCE.NS","TITAN.NS","NESTLEIND.NS",
        "SUNPHARMA.NS","TECHM.NS","POWERGRID.NS","NTPC.NS","HCLTECH.NS",
        # Brésil
        "VALE3.SA","PETR4.SA","ITUB4.SA","BBDC4.SA","ABEV3.SA",
        "B3SA3.SA","WEGE3.SA","RENT3.SA","LREN3.SA","HAPV3.SA",
        "RDOR3.SA","PRIO3.SA","CSAN3.SA","GGBR4.SA","SBSP3.SA",
        # Mexique
        "AMXL.MX","FEMSAUBD.MX","WALMEX.MX","GMEXICOB.MX","CEMEXCPO.MX",
        "GFINBURO.MX","BIMBOA.MX","GRUMAB.MX",
        # Afrique du Sud
        "NPN.JO","BTI.JO","SBK.JO","FSR.JO","ABG.JO",
        "SOL.JO","AGL.JO","MTN.JO","VOD.JO","NED.JO",
    ],
}

# ─────────────────────────────────────────────
# TEST UNITAIRE ROBUSTE
# ─────────────────────────────────────────────

def test_ticker(ticker, zone):
    """
    Utilise Ticker.history() — plus fiable que yf.download() pour les tickers
    internationaux. Vérifie qu'on obtient au moins 20 jours de données sur 1 mois.
    """
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period="1mo", auto_adjust=True)
        valid = hist is not None and len(hist) >= 5
        return ticker, zone, valid, None
    except Exception as e:
        return ticker, zone, False, str(e)


def validate_all(tickers_dict, max_workers=10):
    tasks = [(t, zone) for zone, tlist in tickers_dict.items() for t in tlist]
    results = []
    total = len(tasks)

    print(f"Test de {total} tickers...\n")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(test_ticker, t, z): (t, z) for t, z in tasks}
        done = 0
        for future in as_completed(futures):
            ticker, zone, valid, err = future.result()
            results.append({"ticker": ticker, "zone": zone, "valid": valid, "error": err})
            done += 1
            status = "✓" if valid else "✗"
            print(f"  [{done:>3}/{total}] {status} {ticker:<25} ({zone})")

    return pd.DataFrame(results)


# ─────────────────────────────────────────────
# DIAGNOSTIC — teste 5 tickers connus d'abord
# ─────────────────────────────────────────────

def quick_diagnostic():
    print("=== Diagnostic rapide (5 tickers connus) ===")
    test_cases = ["AAPL", "MC.PA", "7203.T", "RELIANCE.NS", "VALE3.SA"]
    for ticker in test_cases:
        t = yf.Ticker(ticker)
        hist = t.history(period="1mo")
        n = len(hist)
        print(f"  {ticker:<20} → {n} lignes {'✓' if n > 0 else '✗ PROBLÈME RÉSEAU ?'}")
    print()


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":

    # 1. Diagnostic avant de tout lancer
    quick_diagnostic()

    # 2. Validation complète
    df = validate_all(TICKERS, max_workers=10)

    valid   = df[df["valid"]].copy()
    invalid = df[~df["valid"]].copy()

    print(f"\n{'='*55}")
    print(f"  Tickers valides   : {len(valid)}")
    print(f"  Tickers invalides : {len(invalid)}")
    print(f"{'='*55}")

    if len(valid) == 0:
        print("\n⚠️  AUCUN ticker valide détecté !")
        print("   Causes possibles :")
        print("   1. Pas de connexion internet")
        print("   2. yfinance bloqué (proxy/firewall)")
        print("   3. Version yfinance trop ancienne → pip install --upgrade yfinance")
        print("\nErreurs des 5 premiers :")
        for _, row in invalid.head(5).iterrows():
            print(f"   {row['ticker']}: {row['error']}")
    else:
        print("\nRépartition par zone :")
        print(valid.groupby("zone")["ticker"].count().to_string())

        # Export fichiers
        valid[["ticker","zone"]].to_csv("valid_tickers.csv", index=False)

        with open("valid_tickers.txt", "w") as f:
            for t in valid["ticker"].tolist():
                f.write(t + "\n")

        with open("valid_tickers_python.py", "w") as f:
            tlist = valid["ticker"].tolist()
            f.write(f"# {len(tlist)} tickers validés sur yfinance\n\n")
            f.write("VALID_TICKERS = [\n")
            for i, t in enumerate(tlist):
                sep = "," if i < len(tlist) - 1 else ""
                f.write(f'    "{t}"{sep}\n')
            f.write("]\n")

        print("\n✓ Fichiers exportés :")
        print("   valid_tickers.csv")
        print("   valid_tickers.txt")
        print("   valid_tickers_python.py")

        if len(invalid) > 0:
            print(f"\nTickers invalides ({len(invalid)}) :")
            for _, row in invalid.iterrows():
                print(f"   ✗ {row['ticker']:<25} ({row['zone']})")
