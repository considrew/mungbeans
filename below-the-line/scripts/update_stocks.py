#!/usr/bin/env python3
"""
Below The Line - Weekly Stock Data Pipeline

Fetches weekly price data from Yahoo Finance, calculates:
- 200-week moving average
- Distance from 200WMA (%)
- Week-over-week directional change
- 14-week RSI
- Historical touches of the 200WMA
- Yartseva multibagger metrics
- Buffett quality metrics (ROE, debt, margins)
- Share buyback/dilution tracking

Run weekly on Saturday to capture Friday close data.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Tuple

import numpy as np
import pandas as pd
import yfinance as yf


class NumpyEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles numpy/pandas types."""
    def default(self, obj):
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        return super().default(obj)

# Configuration
OUTPUT_DIR = Path(__file__).parent.parent / 'assets' / 'data'
COMPANIES_FILE = OUTPUT_DIR / 'companies.json'


def load_company_metadata():
    """Load company names, sectors, and IR URLs from reference file."""
    if COMPANIES_FILE.exists():
        with open(COMPANIES_FILE) as f:
            return json.load(f)
    return {}


# Dividend Aristocrats - 25+ consecutive years of dividend increases
DIVIDEND_ARISTOCRATS = {
    'ABT', 'ABBV', 'AFL', 'APD', 'ALB', 'AMCR', 'AOS', 'ADP', 'BDX', 'BRO',
    'BF-B', 'CAH', 'CAT', 'CVX', 'CB', 'CINF', 'CTAS', 'CLX', 'KO', 'CL',
    'ED', 'DOV', 'ECL', 'EMR', 'ESS', 'EXPD', 'XOM', 'FAST', 'FRT', 'BEN',
    'GD', 'GPC', 'GWW', 'HRL', 'ITW', 'JNJ', 'KMB', 'LEG', 'LIN', 'LOW',
    'MKC', 'MCD', 'MDT', 'NEE', 'NUE', 'PNR', 'PEP', 'PPG', 'PG',
    'O', 'ROP', 'SPGI', 'SHW', 'SWK', 'SYY', 'TROW', 'TGT', 'WMT', 'WBA',
    'WST', 'CHD', 'ATO', 'CHRW', 'IBM', 'NDSN', 'SJM', 'AWK', 'KVUE'
}


STOCK_UNIVERSE = [
    'A',
    'AA',
    'AAL',
    'AAOI',
    'AAON',
    'AAP',
    'AAPL',
    'ABBV',
    'ABCB',
    'ABG',
    'ABM',
    'ABNB',
    'ABR',
    'ABT',
    'ACAD',
    'ACB',
    'ACCO',
    'ACGL',
    'ACHR',
    'ACLS',
    'ACM',
    'ACMR',
    'ACN',
    'ADBE',
    'ADC',
    'ADEA',
    'ADM',
    'ADMA',
    'ADNT',
    'ADP',
    'ADPT',
    'ADSK',
    'ADTN',
    'ADUS',
    'AEE',
    'AEIS',
    'AEO',
    'AEP',
    'AES',
    'AFG',
    'AFL',
    'AFRM',
    'AGCO',
    'AGNC',
    'AGO',
    'AHCO',
    'AHH',
    'AI',
    'AIG',
    'AIT',
    'AIZ',
    'AJG',
    'AKAM',
    'AKRO',
    'ALB',
    'ALE',
    'ALEC',
    'ALEX',
    'ALGM',
    'ALGN',
    'ALGT',
    'ALHC',
    'ALK',
    'ALKS',
    'ALL',
    'ALLE',
    'ALLO',
    'ALLY',
    'ALNY',
    'ALRM',
    'ALRS',
    'AM',
    'AMAT',
    'AMBA',
    'AMC',
    'AMCR',
    'AMD',
    'AME',
    'AMG',
    'AMGN',
    'AMKR',
    'AMP',
    'AMPH',
    'AMPL',
    'AMRC',
    'AMRK',
    'AMRX',
    'AMT',
    'AMWD',
    'AMZN',
    'ANAB',
    'ANDE',
    'ANET',
    'ANF',
    'ANIK',
    'ANIP',
    'ANSS',
    'AON',
    'AORT',
    'AOS',
    'AOSL',
    'APA',
    'APAM',
    'APD',
    'APEI',
    'APH',
    'APLD',
    'APLS',
    'APP',
    'APPF',
    'APPN',
    'APPS',
    'APTV',
    'AR',
    'ARAY',
    'ARCB',
    'ARCT',
    'ARDX',
    'ARE',
    'ARES',
    'ARHS',
    'ARKK',
    'ARLP',
    'ARM',
    'AROC',
    'AROW',
    'ARQT',
    'ARR',
    'ARRY',
    'ARTNA',
    'ARVN',
    'ARW',
    'ASB',
    'ASGN',
    'ASIX',
    'ASLE',
    'ASML',
    'ASPN',
    'ASTE',
    'ASTS',
    'ATEC',
    'ATEN',
    'ATHM',
    'ATI',
    'ATNI',
    'ATO',
    'ATRA',
    'ATRC',
    'ATRO',
    'AUB',
    'AVA',
    'AVAH',
    'AVAV',
    'AVB',
    'AVGO',
    'AVNS',
    'AVNW',
    'AVO',
    'AVPT',
    'AVTR',
    'AVY',
    'AWI',
    'AWK',
    'AWR',
    'AX',
    'AXGN',
    'AXON',
    'AXP',
    'AXS',
    'AXSM',
    'AXTA',
    'AXTI',
    'AYI',
    'AZN',
    'AZO',
    'AZZ',
    'B',
    'BA',
    'BABA',
    'BAC',
    'BALL',
    'BALY',
    'BANC',
    'BANF',
    'BANR',
    'BAX',
    'BBAI',
    'BBY',
    'BC',
    'BCC',
    'BCE',
    'BCML',
    'BCO',
    'BCPC',
    'BCRX',
    'BDC',
    'BDX',
    'BE',
    'BEAM',
    'BEN',
    'BF-B',
    'BG',
    'BGC',
    'BGS',
    'BHB',
    'BHE',
    'BHF',
    'BHP',
    'BHVN',
    'BIDU',
    'BIIB',
    'BILI',
    'BILL',
    'BIO',
    'BITF',
    'BITO',
    'BITX',
    'BJ',
    'BJRI',
    'BK',
    'BKD',
    'BKE',
    'BKH',
    'BKNG',
    'BKR',
    'BKSY',
    'BKU',
    'BL',
    'BLBD',
    'BLCO',
    'BLDR',
    'BLFS',
    'BLK',
    'BLKB',
    'BLMN',
    'BLNK',
    'BMBL',
    'BMI',
    'BMRC',
    'BMRN',
    'BMY',
    'BNL',
    'BNTX',
    'BOH',
    'BOKF',
    'BOOM',
    'BOOT',
    'BOX',
    'BP',
    'BPOP',
    'BR',
    'BRC',
    'BRCC',
    'BRK-B',
    'BRKR',
    'BRO',
    'BRSP',
    'BRX',
    'BRY',
    'BSM',
    'BSRR',
    'BSX',
    'BTAI',
    'BTDR',
    'BTG',
    'BTI',
    'BTU',
    'BURL',
    'BUSE',
    'BV',
    'BWA',
    'BWFG',
    'BWXT',
    'BX',
    'BXC',
    'BXMT',
    'BXP',
    'BY',
    'BYD',
    'BYND',
    'C',
    'CAC',
    'CACI',
    'CADE',
    'CAG',
    'CAH',
    'CAKE',
    'CALM',
    'CAMP',
    'CAR',
    'CARG',
    'CARR',
    'CASS',
    'CAT',
    'CATC',
    'CATO',
    'CATY',
    'CB',
    'CBAN',
    'CBFV',
    'CBL',
    'CBNK',
    'CBOE',
    'CBRE',
    'CBRL',
    'CBT',
    'CBU',
    'CBZ',
    'CCBG',
    'CCI',
    'CCJ',
    'CCL',
    'CCNE',
    'CCOI',
    'CCS',
    'CCSI',
    'CDNA',
    'CDNS',
    'CDRE',
    'CDW',
    'CDXS',
    'CE',
    'CECO',
    'CEG',
    'CELH',
    'CENT',
    'CENTA',
    'CENX',
    'CERS',
    'CERT',
    'CEVA',
    'CF',
    'CFG',
    'CFLT',
    'CFR',
    'CG',
    'CGC',
    'CGNX',
    'CHCT',
    'CHD',
    'CHDN',
    'CHE',
    'CHEF',
    'CHGG',
    'CHH',
    'CHPT',
    'CHRD',
    'CHRS',
    'CHRW',
    'CHTR',
    'CHWY',
    'CI',
    'CIEN',
    'CIFR',
    'CIGI',
    'CIM',
    'CINF',
    'CIVB',
    'CIVI',
    'CL',
    'CLB',
    'CLBK',
    'CLDT',
    'CLF',
    'CLFD',
    'CLH',
    'CLNE',
    'CLOV',
    'CLPR',
    'CLSK',
    'CLVT',
    'CLX',
    'CMA',
    'CMC',
    'CMCO',
    'CMCSA',
    'CME',
    'CMG',
    'CMI',
    'CMS',
    'CMT',
    'CNA',
    'CNC',
    'CNDT',
    'CNMD',
    'CNNE',
    'CNO',
    'CNOB',
    'CNP',
    'CNTY',
    'CNX',
    'CNXC',
    'COF',
    'COGT',
    'COHR',
    'COHU',
    'COIN',
    'COKE',
    'COLL',
    'COLM',
    'COMM',
    'COMP',
    'COO',
    'COP',
    'COR',
    'CORT',
    'CORZ',
    'COST',
    'COUR',
    'CPAY',
    'CPB',
    'CPK',
    'CPNG',
    'CPRT',
    'CPRX',
    'CPT',
    'CQP',
    'CR',
    'CRC',
    'CRI',
    'CRK',
    'CRL',
    'CRM',
    'CRMD',
    'CRMT',
    'CRNX',
    'CRON',
    'CROX',
    'CRS',
    'CRTO',
    'CRUS',
    'CRVL',
    'CRWD',
    'CSCO',
    'CSGP',
    'CSGS',
    'CSTL',
    'CSTM',
    'CSV',
    'CSWC',
    'CSX',
    'CTAS',
    'CTLT',
    'CTO',
    'CTRA',
    'CTSH',
    'CTVA',
    'CUBI',
    'CUK',
    'CURLF',
    'CUZ',
    'CVBF',
    'CVCO',
    'CVGI',
    'CVGW',
    'CVLG',
    'CVLT',
    'CVNA',
    'CVS',
    'CVX',
    'CW',
    'CWH',
    'CWST',
    'CXM',
    'CXW',
    'CYBR',
    'CYH',
    'CYRX',
    'CYTK',
    'CZR',
    'D',
    'DAKT',
    'DAL',
    'DAN',
    'DAR',
    'DAWN',
    'DAY',
    'DBD',
    'DBI',
    'DBX',
    'DCBO',
    'DCGO',
    'DCI',
    'DD',
    'DDD',
    'DDOG',
    'DDS',
    'DE',
    'DECK',
    'DELL',
    'DEO',
    'DFIN',
    'DFS',
    'DG',
    'DGII',
    'DGX',
    'DHC',
    'DHI',
    'DHR',
    'DHT',
    'DINO',
    'DIOD',
    'DIS',
    'DJT',
    'DKNG',
    'DKS',
    'DLB',
    'DLHC',
    'DLR',
    'DLTR',
    'DLX',
    'DNA',
    'DNLI',
    'DNN',
    'DNOW',
    'DNUT',
    'DOC',
    'DOCN',
    'DOCS',
    'DOCU',
    'DORM',
    'DOV',
    'DOW',
    'DOX',
    'DPZ',
    'DRH',
    'DRI',
    'DRVN',
    'DSGX',
    'DTE',
    'DUK',
    'DUOL',
    'DV',
    'DVA',
    'DVAX',
    'DVN',
    'DX',
    'DXC',
    'DXCM',
    'DY',
    'EA',
    'EAT',
    'EBAY',
    'EBC',
    'ECL',
    'ECPG',
    'ECVT',
    'ED',
    'EE',
    'EEFT',
    'EFSC',
    'EFX',
    'EG',
    'EGBN',
    'EGO',
    'EGP',
    'EHC',
    'EIX',
    'EL',
    'ELV',
    'EMBC',
    'EMN',
    'EMR',
    'ENB',
    'ENOV',
    'ENPH',
    'ENS',
    'ENSG',
    'ENTA',
    'ENTG',
    'ENVX',
    'EOG',
    'EPAC',
    'EPAM',
    'EPD',
    'EPM',
    'EPRT',
    'EQBK',
    'EQIX',
    'EQNR',
    'EQR',
    'EQT',
    'ERIE',
    'ERII',
    'ES',
    'ESCA',
    'ESI',
    'ESNT',
    'ESPR',
    'ESRT',
    'ESS',
    'ESTA',
    'ESTC',
    'ET',
    'ETD',
    'ETHE',
    'ETN',
    'ETR',
    'ETSY',
    'EVC',
    'EVER',
    'EVGO',
    'EVH',
    'EVLV',
    'EVR',
    'EVRG',
    'EVTC',
    'EW',
    'EWBC',
    'EXAS',
    'EXC',
    'EXEL',
    'EXLS',
    'EXP',
    'EXPD',
    'EXPE',
    'EXPI',
    'EXPO',
    'EXR',
    'EXTR',
    'F',
    'FAF',
    'FANG',
    'FAST',
    'FATE',
    'FBIN',
    'FBK',
    'FCEL',
    'FCNCA',
    'FCX',
    'FDS',
    'FDX',
    'FE',
    'FELE',
    'FFIN',
    'FFIV',
    'FHN',
    'FI',
    'FIBK',
    'FICO',
    'FIGS',
    'FIS',
    'FITB',
    'FIVE',
    'FIVN',
    'FIX',
    'FIZZ',
    'FLO',
    'FLWS',
    'FMBH',
    'FMC',
    'FMX',
    'FN',
    'FNB',
    'FND',
    'FNF',
    'FOLD',
    'FORM',
    'FORR',
    'FOUR',
    'FOX',
    'FOXA',
    'FOXF',
    'FRME',
    'FROG',
    'FRPT',
    'FRSH',
    'FRT',
    'FSLR',
    'FSLY',
    'FTI',
    'FTNT',
    'FTV',
    'FUL',
    'FUTU',
    'FWONK',
    'FWRD',
    'FWRG',
    'G',
    'GABC',
    'GATX',
    'GBCI',
    'GBTC',
    'GCO',
    'GD',
    'GDDY',
    'GDEN',
    'GDYN',
    'GE',
    'GEF',
    'GEHC',
    'GEN',
    'GENI',
    'GEO',
    'GERN',
    'GEV',
    'GEVO',
    'GFF',
    'GFS',
    'GGG',
    'GHC',
    'GIII',
    'GIL',
    'GILD',
    'GIS',
    'GL',
    'GLBE',
    'GLDD',
    'GLW',
    'GM',
    'GMAB',
    'GME',
    'GMRE',
    'GNRC',
    'GNTX',
    'GNW',
    'GO',
    'GOEV',
    'GOGO',
    'GOLF',
    'GOOD',
    'GOOG',
    'GOOGL',
    'GPC',
    'GPI',
    'GPK',
    'GPMT',
    'GPN',
    'GPRE',
    'GPRO',
    'GRAB',
    'GRBK',
    'GRC',
    'GRFS',
    'GRMN',
    'GRPN',
    'GRWG',
    'GS',
    'GSAT',
    'GSBC',
    'GSK',
    'GTBIF',
    'GTLB',
    'GVA',
    'GWW',
    'GXO',
    'H',
    'HAE',
    'HAFC',
    'HAIN',
    'HAL',
    'HALO',
    'HAS',
    'HASI',
    'HBAN',
    'HBB',
    'HBCP',
    'HBI',
    'HBM',
    'HBNC',
    'HBT',
    'HCA',
    'HCAT',
    'HCC',
    'HCI',
    'HCKT',
    'HCSG',
    'HD',
    'HDB',
    'HE',
    'HEI',
    'HEI-A',
    'HELE',
    'HES',
    'HGV',
    'HI',
    'HIFS',
    'HIG',
    'HII',
    'HL',
    'HLF',
    'HLI',
    'HLMN',
    'HLNE',
    'HLT',
    'HLX',
    'HMN',
    'HNI',
    'HOFT',
    'HOLX',
    'HOMB',
    'HON',
    'HOOD',
    'HOPE',
    'HOV',
    'HP',
    'HPE',
    'HPK',
    'HPQ',
    'HQI',
    'HQY',
    'HRI',
    'HRL',
    'HRMY',
    'HRTG',
    'HRTX',
    'HSIC',
    'HST',
    'HSTM',
    'HSY',
    'HTBK',
    'HTGC',
    'HTH',
    'HTLD',
    'HUBB',
    'HUBG',
    'HUBS',
    'HUM',
    'HUN',
    'HURN',
    'HUT',
    'HVT',
    'HWBK',
    'HWC',
    'HWKN',
    'HWM',
    'HXL',
    'HY',
    'HYLN',
    'HZO',
    'IAC',
    'IART',
    'IBCP',
    'IBEX',
    'IBIT',
    'IBKR',
    'IBM',
    'IBN',
    'IBOC',
    'IBP',
    'ICE',
    'ICFI',
    'ICHR',
    'ICLR',
    'ICUI',
    'IDCC',
    'IDT',
    'IDXX',
    'IESC',
    'IEX',
    'IFF',
    'IHG',
    'IHRT',
    'IIIV',
    'ILMN',
    'ILPT',
    'IMMR',
    'IMVT',
    'INBK',
    'INCY',
    'INDB',
    'INFU',
    'INFY',
    'ING',
    'INGR',
    'INO',
    'INOD',
    'INSG',
    'INSM',
    'INSP',
    'INSW',
    'INTA',
    'INTC',
    'INTU',
    'INVH',
    'IONQ',
    'IONS',
    'IOVA',
    'IP',
    'IPAR',
    'IPG',
    'IPGP',
    'IQ',
    'IQV',
    'IR',
    'IRBT',
    'IRDM',
    'IRM',
    'IRMD',
    'IRWD',
    'ISRG',
    'IT',
    'ITGR',
    'ITRI',
    'ITT',
    'ITUB',
    'ITW',
    'IVA',
    'IVR',
    'IVT',
    'IVZ',
    'IX',
    'J',
    'JACK',
    'JAMF',
    'JAZZ',
    'JBGS',
    'JBHT',
    'JBI',
    'JBL',
    'JBLU',
    'JBSS',
    'JCI',
    'JD',
    'JEF',
    'JELD',
    'JHG',
    'JJSF',
    'JKHY',
    'JKS',
    'JLL',
    'JNJ',
    'JNPR',
    'JOBY',
    'JOE',
    'JPM',
    'K',
    'KAI',
    'KALU',
    'KAR',
    'KB',
    'KBH',
    'KBR',
    'KDP',
    'KE',
    'KEX',
    'KEY',
    'KEYS',
    'KFRC',
    'KFY',
    'KGS',
    'KHC',
    'KIM',
    'KKR',
    'KLAC',
    'KLIC',
    'KMB',
    'KMI',
    'KMT',
    'KMX',
    'KN',
    'KNSA',
    'KNSL',
    'KNX',
    'KO',
    'KOD',
    'KOS',
    'KR',
    'KRC',
    'KREF',
    'KRG',
    'KROS',
    'KRP',
    'KRUS',
    'KRYS',
    'KSS',
    'KTOS',
    'KURA',
    'KVUE',
    'KVYO',
    'KW',
    'KWR',
    'KYMR',
    'L',
    'LADR',
    'LAKE',
    'LAMR',
    'LAND',
    'LASR',
    'LAUR',
    'LAW',
    'LAZ',
    'LAZR',
    'LB',
    'LBRDA',
    'LBRDK',
    'LBRT',
    'LC',
    'LCID',
    'LCII',
    'LCNB',
    'LCUT',
    'LDI',
    'LDOS',
    'LE',
    'LEA',
    'LEG',
    'LEGH',
    'LEN',
    'LESL',
    'LEU',
    'LFMD',
    'LFST',
    'LFUS',
    'LGIH',
    'LGND',
    'LH',
    'LHX',
    'LI',
    'LII',
    'LIN',
    'LIND',
    'LITE',
    'LIVN',
    'LKFN',
    'LKQ',
    'LLY',
    'LMAT',
    'LMND',
    'LMNR',
    'LMT',
    'LNC',
    'LNG',
    'LNN',
    'LNT',
    'LNTH',
    'LNZA',
    'LOB',
    'LOCO',
    'LOGI',
    'LOOP',
    'LOPE',
    'LOVE',
    'LOW',
    'LPLA',
    'LPX',
    'LRCX',
    'LSCC',
    'LSPD',
    'LSTR',
    'LTC',
    'LULU',
    'LUMN',
    'LUNA',
    'LUNG',
    'LUNR',
    'LUV',
    'LVS',
    'LW',
    'LXRX',
    'LXU',
    'LYB',
    'LYV',
    'LZB',
    'M',
    'MA',
    'MAA',
    'MAIN',
    'MANH',
    'MAR',
    'MARA',
    'MAS',
    'MASI',
    'MAT',
    'MATV',
    'MATW',
    'MATX',
    'MAX',
    'MAXN',
    'MBC',
    'MBCN',
    'MBIN',
    'MBUU',
    'MBWM',
    'MC',
    'MCB',
    'MCBS',
    'MCD',
    'MCFT',
    'MCHP',
    'MCK',
    'MCO',
    'MCRI',
    'MCW',
    'MCY',
    'MDB',
    'MDGL',
    'MDLZ',
    'MDRX',
    'MDT',
    'MDU',
    'MDXG',
    'MEDP',
    'MEG',
    'MEI',
    'MELI',
    'MET',
    'META',
    'MFA',
    'MG',
    'MGEE',
    'MGM',
    'MGNI',
    'MGNX',
    'MGPI',
    'MGTX',
    'MHK',
    'MHO',
    'MIDD',
    'MIND',
    'MIRM',
    'MITK',
    'MKC',
    'MKSI',
    'MKTX',
    'MLCO',
    'MLI',
    'MLM',
    'MLR',
    'MMC',
    'MMM',
    'MMSI',
    'MNKD',
    'MNR',
    'MNRO',
    'MNST',
    'MNTK',
    'MNTS',
    'MO',
    'MOD',
    'MOFG',
    'MOH',
    'MOS',
    'MOV',
    'MPAA',
    'MPB',
    'MPC',
    'MPLX',
    'MPW',
    'MPWR',
    'MQ',
    'MRCY',
    'MRK',
    'MRM',
    'MRNA',
    'MRO',
    'MRSN',
    'MRTN',
    'MRUS',
    'MRVI',
    'MRVL',
    'MS',
    'MSA',
    'MSBI',
    'MSCI',
    'MSEX',
    'MSFT',
    'MSGE',
    'MSGS',
    'MSI',
    'MSM',
    'MSTR',
    'MSTU',
    'MT',
    'MTB',
    'MTCH',
    'MTD',
    'MTDR',
    'MTG',
    'MTH',
    'MTLS',
    'MTN',
    'MTRN',
    'MTSI',
    'MTX',
    'MU',
    'MUFG',
    'MUR',
    'MUSA',
    'MUX',
    'MVBF',
    'MVST',
    'MWA',
    'MXCT',
    'MXL',
    'MYE',
    'MYFW',
    'MYGN',
    'MYRG',
    'NABL',
    'NATH',
    'NATR',
    'NAVI',
    'NBHC',
    'NBIS',
    'NBIX',
    'NBN',
    'NBR',
    'NBTB',
    'NCLH',
    'NCMI',
    'NCNO',
    'NCSM',
    'NDAQ',
    'NDLS',
    'NDSN',
    'NE',
    'NEE',
    'NEM',
    'NEOG',
    'NEON',
    'NESR',
    'NET',
    'NEU',
    'NEWT',
    'NEXT',
    'NFBK',
    'NFE',
    'NFG',
    'NFLX',
    'NG',
    'NGD',
    'NGS',
    'NGVC',
    'NGVT',
    'NHC',
    'NHI',
    'NI',
    'NICE',
    'NIO',
    'NJR',
    'NKE',
    'NL',
    'NLY',
    'NMFC',
    'NMIH',
    'NMM',
    'NMRA',
    'NMRK',
    'NNI',
    'NNN',
    'NOC',
    'NODK',
    'NOG',
    'NOK',
    'NOV',
    'NOVT',
    'NOW',
    'NPK',
    'NPO',
    'NRC',
    'NRDS',
    'NREF',
    'NRG',
    'NRIM',
    'NRIX',
    'NSA',
    'NSC',
    'NSIT',
    'NSP',
    'NSSC',
    'NTAP',
    'NTCT',
    'NTES',
    'NTGR',
    'NTIC',
    'NTLA',
    'NTNX',
    'NTRA',
    'NTRS',
    'NTST',
    'NU',
    'NUE',
    'NUS',
    'NUVB',
    'NVAX',
    'NVCR',
    'NVDA',
    'NVEC',
    'NVGS',
    'NVO',
    'NVR',
    'NVS',
    'NVST',
    'NVT',
    'NWBI',
    'NWE',
    'NWFL',
    'NWL',
    'NWN',
    'NWPX',
    'NWS',
    'NWSA',
    'NX',
    'NXPI',
    'NXRT',
    'NXST',
    'NXTC',
    'NYT',
    'O',
    'OBT',
    'OC',
    'OCFC',
    'OCGN',
    'OCSL',
    'OCUL',
    'ODC',
    'ODFL',
    'ODP',
    'OEC',
    'OFG',
    'OFIX',
    'OFLX',
    'OGE',
    'OGI',
    'OGN',
    'OGS',
    'OHI',
    'OI',
    'OII',
    'OIS',
    'OKE',
    'OKTA',
    'OLLI',
    'OLN',
    'OLP',
    'OM',
    'OMC',
    'OMCL',
    'OMER',
    'OMF',
    'OMI',
    'ON',
    'ONB',
    'ONEW',
    'ONTO',
    'OPBK',
    'OPEN',
    'OPFI',
    'OPK',
    'OPRA',
    'OPRT',
    'OPY',
    'ORA',
    'ORC',
    'ORCL',
    'ORGO',
    'ORI',
    'ORIC',
    'ORLA',
    'ORLY',
    'ORN',
    'ORRF',
    'OSIS',
    'OSK',
    'OSPN',
    'OSUR',
    'OSW',
    'OTIS',
    'OTLY',
    'OTTR',
    'OUST',
    'OUT',
    'OVBC',
    'OVV',
    'OWL',
    'OXM',
    'OXY',
    'OZK',
    'PAA',
    'PAAS',
    'PAC',
    'PACB',
    'PACK',
    'PAG',
    'PAGP',
    'PAGS',
    'PAHC',
    'PANW',
    'PAR',
    'PARA',
    'PARR',
    'PATH',
    'PATK',
    'PAYC',
    'PAYS',
    'PAYX',
    'PB',
    'PBA',
    'PBF',
    'PBFS',
    'PBH',
    'PBI',
    'PBR',
    'PCAR',
    'PCB',
    'PCG',
    'PCH',
    'PCOR',
    'PCRX',
    'PCTY',
    'PCVX',
    'PD',
    'PDD',
    'PEG',
    'PENN',
    'PEP',
    'PFE',
    'PFG',
    'PFGC',
    'PG',
    'PGR',
    'PH',
    'PHM',
    'PINS',
    'PKG',
    'PL',
    'PLD',
    'PLTR',
    'PLUG',
    'PM',
    'PNC',
    'PNFP',
    'PNR',
    'PNW',
    'PODD',
    'POOL',
    'POWI',
    'PPG',
    'PPL',
    'PR',
    'PRI',
    'PRTA',
    'PRU',
    'PSA',
    'PSX',
    'PTC',
    'PTON',
    'PUBM',
    'PWR',
    'PXD',
    'PYPL',
    'QBTS',
    'QCOM',
    'QLYS',
    'QRVO',
    'QS',
    'QUBT',
    'RARE',
    'RBC',
    'RBLX',
    'RCAT',
    'RCL',
    'RDDT',
    'RDW',
    'REG',
    'REGN',
    'RF',
    'RGTI',
    'RH',
    'RIO',
    'RIOT',
    'RITM',
    'RIVN',
    'RJF',
    'RKLB',
    'RL',
    'RLI',
    'RMD',
    'RNG',
    'ROK',
    'ROKU',
    'ROL',
    'ROOT',
    'ROP',
    'ROST',
    'RPD',
    'RRC',
    'RRX',
    'RS',
    'RSG',
    'RSI',
    'RTX',
    'RUN',
    'RVLV',
    'S',
    'SAIC',
    'SAN',
    'SAP',
    'SBAC',
    'SBCF',
    'SBUX',
    'SCHD',
    'SCHW',
    'SE',
    'SEDG',
    'SEIC',
    'SFNC',
    'SHEL',
    'SHG',
    'SHOP',
    'SHW',
    'SIRI',
    'SITE',
    'SJM',
    'SLB',
    'SLDP',
    'SMCI',
    'SMFG',
    'SMR',
    'SMTC',
    'SNA',
    'SNAP',
    'SNDL',
    'SNOW',
    'SNPS',
    'SNV',
    'SNY',
    'SO',
    'SOFI',
    'SOLV',
    'SONY',
    'SOUN',
    'SPCE',
    'SPG',
    'SPGI',
    'SPOT',
    'SRE',
    'SRPT',
    'SSB',
    'STE',
    'STEM',
    'STLD',
    'STT',
    'STX',
    'STZ',
    'SWK',
    'SWKS',
    'SYF',
    'SYK',
    'SYNA',
    'SYY',
    'T',
    'TAP',
    'TCBI',
    'TCOM',
    'TDG',
    'TDY',
    'TEAM',
    'TECH',
    'TEL',
    'TEM',
    'TENB',
    'TER',
    'TEVA',
    'TFC',
    'TFX',
    'TGT',
    'TGTX',
    'TIGR',
    'TJX',
    'TLRY',
    'TM',
    'TME',
    'TMO',
    'TMUS',
    'TOL',
    'TPR',
    'TRGP',
    'TRIP',
    'TRMB',
    'TROW',
    'TRV',
    'TSCO',
    'TSLA',
    'TSM',
    'TSN',
    'TT',
    'TTC',
    'TTD',
    'TTE',
    'TTWO',
    'TWLO',
    'TXN',
    'TXT',
    'TYL',
    'U',
    'UAL',
    'UBER',
    'UBSI',
    'UDR',
    'UHS',
    'UL',
    'ULTA',
    'UNH',
    'UNP',
    'UPS',
    'UPST',
    'URBN',
    'URI',
    'USB',
    'USFD',
    'UTHR',
    'UUUU',
    'V',
    'VALE',
    'VEEV',
    'VICI',
    'VLO',
    'VLTO',
    'VMC',
    'VNDA',
    'VRNS',
    'VRSK',
    'VRSN',
    'VRT',
    'VRTX',
    'VST',
    'VTR',
    'VTRS',
    'VXRT',
    'VZ',
    'W',
    'WAB',
    'WAL',
    'WAT',
    'WBA',
    'WBD',
    'WCC',
    'WDAY',
    'WDC',
    'WEC',
    'WELL',
    'WFC',
    'WH',
    'WIT',
    'WK',
    'WKHS',
    'WLK',
    'WM',
    'WMB',
    'WMT',
    'WRB',
    'WRK',
    'WSM',
    'WST',
    'WTFC',
    'WTW',
    'WULF',
    'WWD',
    'WY',
    'WYNN',
    'XEL',
    'XENE',
    'XOM',
    'XPEV',
    'XYL',
    'YUM',
    'Z',
    'ZBH',
    'ZBRA',
    'ZION',
    'ZS',
    'ZTS',
    'ZWS',
]


def fetch_weekly_data(symbol: str) -> Optional[pd.DataFrame]:
    """Fetch weekly price data from Yahoo Finance."""
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="max", interval="1wk")
        
        if df.empty:
            print(f"  ✗ No data returned for {symbol}")
            return None
        
        df = df.rename(columns={
            'Open': 'open', 'High': 'high', 'Low': 'low',
            'Close': 'close', 'Volume': 'volume'
        })
        df['adjusted_close'] = df['close']
        return df
        
    except Exception as e:
        print(f"  ✗ Error fetching {symbol}: {e}")
        return None


def get_share_change(ticker: yf.Ticker) -> Tuple[Optional[float], Optional[float], Optional[int]]:
    """
    Calculate share count change over time to detect buybacks vs dilution.
    
    Returns:
        - yoy_change: Year-over-year share count change (%)
        - three_yr_change: 3-year share count change (%)
        - current_shares: Current shares outstanding
    
    Negative = buybacks (good)
    Positive = dilution (bad)
    """
    try:
        bs = ticker.balance_sheet
        
        if bs.empty or 'Ordinary Shares Number' not in bs.index:
            return None, None, None
        
        shares = bs.loc['Ordinary Shares Number'].dropna().sort_index(ascending=False)
        
        if len(shares) < 2:
            return None, None, None
        
        current = float(shares.iloc[0])
        
        # 1-year change
        yoy_change = None
        if len(shares) >= 2:
            year_ago = float(shares.iloc[1])
            if year_ago > 0:
                yoy_change = ((current - year_ago) / year_ago) * 100
        
        # 3-year change (use oldest available if < 4 years)
        three_yr_change = None
        if len(shares) >= 4:
            three_yr_ago = float(shares.iloc[3])
            if three_yr_ago > 0:
                three_yr_change = ((current - three_yr_ago) / three_yr_ago) * 100
        elif len(shares) >= 3:
            oldest = float(shares.iloc[-1])
            if oldest > 0:
                three_yr_change = ((current - oldest) / oldest) * 100
        
        return (
            round(yoy_change, 1) if yoy_change is not None else None,
            round(three_yr_change, 1) if three_yr_change is not None else None,
            int(current)
        )
        
    except Exception:
        return None, None, None


def fetch_fundamental_data(symbol: str) -> dict:
    """
    Fetch fundamental data for quality screening.
    
    Includes:
    - Yartseva multibagger metrics (FCF yield, P/B, market cap)
    - Buffett quality metrics (ROE, debt/equity, margins)
    - Share buyback/dilution tracking
    - Dividend info
    """
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        
        # Basic metrics
        market_cap = info.get('marketCap')
        fcf = info.get('freeCashflow')
        book_value = info.get('bookValue')
        price_to_book = info.get('priceToBook')
        profit_margin = info.get('profitMargins')
        operating_margin = info.get('operatingMargins')
        revenue = info.get('totalRevenue')
        
        # Quality metrics (Buffett/Munger style)
        roe = info.get('returnOnEquity')
        debt_to_equity = info.get('debtToEquity')
        gross_margin = info.get('grossMargins')
        current_ratio = info.get('currentRatio')
        dividend_yield = info.get('dividendYield')
        
        # Share buyback/dilution
        shares_yoy_change, shares_3yr_change, shares_outstanding = get_share_change(ticker)
        
        # === DERIVED METRICS ===
        
        fcf_yield = None
        if fcf and market_cap and market_cap > 0:
            fcf_yield = (fcf / market_cap) * 100
        
        book_to_market = None
        if price_to_book and price_to_book > 0:
            book_to_market = 1 / price_to_book
        
        roe_pct = roe * 100 if roe is not None else None
        gross_margin_pct = gross_margin * 100 if gross_margin is not None else None
        
        # === QUALITY FLAGS ===
        
        # Yartseva flags
        # Use bool() to convert numpy.bool_ to Python bool for JSON serialization
        is_small_cap = bool(market_cap is not None and market_cap < 2_000_000_000)
        has_positive_equity = bool(book_value is not None and book_value > 0)
        has_positive_fcf = bool(fcf is not None and fcf > 0)
        
        yartseva_candidate = bool(
            is_small_cap and
            has_positive_equity and
            has_positive_fcf and
            fcf_yield is not None and fcf_yield >= 5 and
            book_to_market is not None and book_to_market >= 0.4
        )
        
        # Buffett quality flags
        low_debt = bool(debt_to_equity is not None and debt_to_equity < 50)
        high_roe = bool(roe_pct is not None and roe_pct > 15)
        wide_moat = bool(gross_margin_pct is not None and gross_margin_pct > 40 and high_roe)
        
        buffett_quality = bool(
            high_roe and
            low_debt and
            has_positive_fcf and
            profit_margin is not None and profit_margin > 0
        )
        
        # Share buyback/dilution flags
        # Buyback: shares decreased by > 2% over 3 years
        # Dilution: shares increased by > 2% over 3 years
        is_buying_back = bool(shares_3yr_change is not None and shares_3yr_change < -2)
        is_diluting = bool(shares_3yr_change is not None and shares_3yr_change > 2)
        
        # Cannibal: aggressive buybacks (> 5% reduction over 3 years)
        is_cannibal = bool(shares_3yr_change is not None and shares_3yr_change < -5)
        
        # Dividend Aristocrat
        is_dividend_aristocrat = symbol in DIVIDEND_ARISTOCRATS
        
        return {
            # Basic
            'market_cap': market_cap,
            'fcf': fcf,
            'fcf_yield': round(fcf_yield, 2) if fcf_yield else None,
            'book_value': round(book_value, 2) if book_value else None,
            'price_to_book': round(price_to_book, 2) if price_to_book else None,
            'book_to_market': round(book_to_market, 3) if book_to_market else None,
            'profit_margin': round(profit_margin * 100, 1) if profit_margin else None,
            'operating_margin': round(operating_margin * 100, 1) if operating_margin else None,
            'revenue': revenue,
            # Quality metrics
            'roe': round(roe_pct, 1) if roe_pct else None,
            'debt_to_equity': round(debt_to_equity, 1) if debt_to_equity else None,
            'gross_margin': round(gross_margin_pct, 1) if gross_margin_pct else None,
            'current_ratio': round(current_ratio, 2) if current_ratio else None,
            'dividend_yield': round(dividend_yield * 100, 2) if dividend_yield else None,
            # Share buyback/dilution
            'shares_outstanding': shares_outstanding,
            'shares_change_yoy': shares_yoy_change,
            'shares_change_3yr': shares_3yr_change,
            # Flags
            'is_small_cap': is_small_cap,
            'has_positive_equity': has_positive_equity,
            'has_positive_fcf': has_positive_fcf,
            'low_debt': low_debt,
            'high_roe': high_roe,
            'wide_moat': wide_moat,
            'buffett_quality': buffett_quality,
            'dividend_aristocrat': is_dividend_aristocrat,
            'yartseva_candidate': yartseva_candidate,
            'is_buying_back': is_buying_back,
            'is_diluting': is_diluting,
            'is_cannibal': is_cannibal,
        }
        
    except Exception as e:
        return {
            'market_cap': None, 'fcf': None, 'fcf_yield': None,
            'book_value': None, 'price_to_book': None, 'book_to_market': None,
            'profit_margin': None, 'operating_margin': None, 'revenue': None,
            'roe': None, 'debt_to_equity': None, 'gross_margin': None,
            'current_ratio': None, 'dividend_yield': None,
            'shares_outstanding': None, 'shares_change_yoy': None, 'shares_change_3yr': None,
            'is_small_cap': False, 'has_positive_equity': False,
            'has_positive_fcf': False, 'low_debt': False, 'high_roe': False,
            'wide_moat': False, 'buffett_quality': False,
            'dividend_aristocrat': symbol in DIVIDEND_ARISTOCRATS,
            'yartseva_candidate': False,
            'is_buying_back': False, 'is_diluting': False, 'is_cannibal': False,
        }


def calculate_rsi(prices: pd.Series, periods: int = 14) -> pd.Series:
    """Calculate RSI (Relative Strength Index)."""
    delta = prices.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=periods).mean()
    avg_loss = loss.rolling(window=periods).mean()
    rs = avg_gain / avg_loss.replace(0, float('inf'))
    rsi = 100 - (100 / (1 + rs))
    return rsi


def find_historical_touches(df: pd.DataFrame) -> List[dict]:
    """Find all historical instances where price crossed below 200WMA."""
    df = df.copy()
    df['below'] = df['adjusted_close'] < df['WMA_200']
    df['cross_below'] = df['below'] & ~df['below'].shift(1).fillna(False)
    
    touches = []
    cross_dates = df[df['cross_below']].index.tolist()
    
    for cross_date in cross_dates:
        touch_start_idx = df.index.get_loc(cross_date)
        subsequent = df.iloc[touch_start_idx:]
        cross_above = subsequent[~subsequent['below']]
        
        if len(cross_above) > 0:
            touch_end_date = cross_above.index[0]
            touch_end_idx = df.index.get_loc(touch_end_date)
            touch_data = df.iloc[touch_start_idx:touch_end_idx]
            weeks_below = len(touch_data)
            min_pct = touch_data['pct_from_wma'].min()
            max_depth = abs(min_pct)
            
            one_year_later_idx = touch_start_idx + 52
            if one_year_later_idx < len(df):
                entry_price = df.iloc[touch_start_idx]['adjusted_close']
                exit_price = df.iloc[one_year_later_idx]['adjusted_close']
                return_1yr = ((exit_price - entry_price) / entry_price) * 100
            else:
                return_1yr = None
            
            touches.append({
                'date': cross_date.strftime('%b %Y'),
                'date_iso': cross_date.strftime('%Y-%m-%d'),
                'weeks_below': int(weeks_below),
                'max_depth': round(float(max_depth), 1),
                'return_1yr': round(float(return_1yr), 1) if return_1yr is not None else None,
                'ongoing': False
            })
        else:
            touch_data = subsequent[subsequent['below']]
            weeks_below = len(touch_data)
            min_pct = touch_data['pct_from_wma'].min()
            max_depth = abs(min_pct)
            
            touches.append({
                'date': cross_date.strftime('%b %Y'),
                'date_iso': cross_date.strftime('%Y-%m-%d'),
                'weeks_below': int(weeks_below),
                'max_depth': round(float(max_depth), 1),
                'return_1yr': None,
                'ongoing': True
            })
    
    return touches


def calculate_stock_signals(symbol: str) -> Optional[dict]:
    """Calculate all signals for a stock including quality metrics."""
    print(f"  Processing {symbol}...")
    
    df = fetch_weekly_data(symbol)
    if df is None:
        return None
    
    if len(df) < 200:
        print(f"  ✗ {symbol}: Only {len(df)} weeks of data (need 200+)")
        if len(df) < 50:
            return None
    
    df['WMA_200'] = df['adjusted_close'].rolling(window=200, min_periods=50).mean()
    df['pct_from_wma'] = ((df['adjusted_close'] - df['WMA_200']) / df['WMA_200']) * 100
    df['wow_change'] = df['pct_from_wma'] - df['pct_from_wma'].shift(1)
    df['RSI_14'] = calculate_rsi(df['adjusted_close'], periods=14)
    
    df_complete = df.dropna(subset=['WMA_200'])
    if len(df_complete) == 0:
        print(f"  ✗ {symbol}: No valid WMA data")
        return None
    
    historical_touches = find_historical_touches(df_complete.copy())
    fundamentals = fetch_fundamental_data(symbol)
    
    latest = df_complete.iloc[-1]
    buy_threshold = latest['WMA_200']
    pct = latest['pct_from_wma']
    
    if pct <= -10:
        zone = 'extreme_value'
    elif pct <= -5:
        zone = 'deep_value'
    elif pct <= 0:
        zone = 'below_line'
    elif pct <= 5:
        zone = 'at_doorstep'
    elif pct <= 10:
        zone = 'getting_close'
    elif pct <= 15:
        zone = 'approaching'
    else:
        zone = 'above'
    
    returns = [t['return_1yr'] for t in historical_touches if t.get('return_1yr') is not None]
    avg_return = round(sum(returns) / len(returns), 1) if returns else None
    avg_weeks = round(sum(t['weeks_below'] for t in historical_touches) / len(historical_touches), 1) if historical_touches else None
    
    # Combo flags (quality + below line = golden opportunities)
    below = pct < 0
    yartseva_below_line = bool(fundamentals['yartseva_candidate'] and below)
    buffett_below_line = bool(fundamentals['buffett_quality'] and below)
    aristocrat_below_line = bool(fundamentals['dividend_aristocrat'] and below)
    cannibal_below_line = bool(fundamentals['is_cannibal'] and below)
    
    result = {
        'symbol': symbol,
        'close': round(float(latest['adjusted_close']), 2),
        'wma_200': round(float(latest['WMA_200']), 2),
        'buy_threshold': round(float(buy_threshold), 2),
        'pct_from_wma': round(float(latest['pct_from_wma']), 2),
        'wow_change': round(float(latest['wow_change']), 2) if pd.notna(latest['wow_change']) else 0.0,
        'rsi_14': round(float(latest['RSI_14']), 1) if pd.notna(latest['RSI_14']) else 50.0,
        'below_line': bool(latest['adjusted_close'] < latest['WMA_200']),
        'approaching': bool(float(latest['wow_change']) < 0) if pd.notna(latest['wow_change']) else False,
        'zone': zone,
        'historical_touches': historical_touches,
        'touch_count': len(historical_touches),
        'avg_return_after_touch': avg_return,
        'avg_weeks_below': avg_weeks,
        # Fundamentals
        'market_cap': fundamentals['market_cap'],
        'fcf': fundamentals['fcf'],
        'fcf_yield': fundamentals['fcf_yield'],
        'book_value': fundamentals['book_value'],
        'price_to_book': fundamentals['price_to_book'],
        'book_to_market': fundamentals['book_to_market'],
        'profit_margin': fundamentals['profit_margin'],
        'operating_margin': fundamentals['operating_margin'],
        # Quality metrics
        'roe': fundamentals['roe'],
        'debt_to_equity': fundamentals['debt_to_equity'],
        'gross_margin': fundamentals['gross_margin'],
        'current_ratio': fundamentals['current_ratio'],
        'dividend_yield': fundamentals['dividend_yield'],
        # Share buyback/dilution
        'shares_outstanding': fundamentals['shares_outstanding'],
        'shares_change_yoy': fundamentals['shares_change_yoy'],
        'shares_change_3yr': fundamentals['shares_change_3yr'],
        # Quality flags
        'is_small_cap': fundamentals['is_small_cap'],
        'has_positive_fcf': fundamentals['has_positive_fcf'],
        'low_debt': fundamentals['low_debt'],
        'high_roe': fundamentals['high_roe'],
        'wide_moat': fundamentals['wide_moat'],
        'buffett_quality': fundamentals['buffett_quality'],
        'dividend_aristocrat': fundamentals['dividend_aristocrat'],
        'yartseva_candidate': fundamentals['yartseva_candidate'],
        'is_buying_back': fundamentals['is_buying_back'],
        'is_diluting': fundamentals['is_diluting'],
        'is_cannibal': fundamentals['is_cannibal'],
        # Combo flags
        'yartseva_below_line': yartseva_below_line,
        'buffett_below_line': buffett_below_line,
        'aristocrat_below_line': aristocrat_below_line,
        'cannibal_below_line': cannibal_below_line,
        # Metadata
        'last_updated': df_complete.index[-1].strftime('%Y-%m-%d'),
        'data_weeks': len(df_complete)
    }
    
    # Status flags for logging
    flags = []
    if cannibal_below_line: flags.append("🦈CANNIBAL")
    if buffett_below_line: flags.append("🏆BUFF")
    if aristocrat_below_line: flags.append("👑ARIST")
    if yartseva_below_line: flags.append("🎯YART")
    if fundamentals['is_diluting']: flags.append("⚠️DILUTE")
    flag_str = " " + " ".join(flags) if flags else ""
    
    print(f"  ✓ {symbol}: {pct:.1f}% from WMA, Zone: {zone}{flag_str}")
    return result


def generate_landing_page_data(stocks: List[dict]) -> dict:
    """Generate summary data for the landing page."""
    below_line = [s for s in stocks if s['below_line']]
    approaching = [s for s in stocks if s['approaching'] and not s['below_line'] and s['pct_from_wma'] <= 15]
    oversold = [s for s in stocks if s['rsi_14'] < 30]
    yartseva = [s for s in stocks if s.get('yartseva_below_line')]
    buffett = [s for s in stocks if s.get('buffett_below_line')]
    aristocrats = [s for s in stocks if s.get('aristocrat_below_line')]
    cannibals = [s for s in stocks if s.get('cannibal_below_line')]
    diluting = [s for s in stocks if s.get('is_diluting')]
    
    return {
        'total_tracked': len(stocks),
        'below_line_count': len(below_line),
        'approaching_count': len(approaching),
        'oversold_count': len(oversold),
        'yartseva_count': len(yartseva),
        'buffett_count': len(buffett),
        'aristocrat_count': len(aristocrats),
        'cannibal_count': len(cannibals),
        'diluting_count': len(diluting),
        'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M UTC')
    }


def main():
    """Main pipeline."""
    print("=" * 60)
    print("Below The Line - Stock Data Pipeline")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    company_metadata = load_company_metadata()
    print(f"Loaded {len(company_metadata)} company records")
    
    all_stocks = []
    errors = []
    
    total = len(STOCK_UNIVERSE)
    for i, symbol in enumerate(STOCK_UNIVERSE):
        print(f"\n[{i+1}/{total}]", end="")
        
        try:
            result = calculate_stock_signals(symbol)
            if result:
                all_stocks.append(result)
            else:
                errors.append(symbol)
        except Exception as e:
            print(f"  ✗ {symbol}: Unexpected error - {e}")
            errors.append(symbol)
        
        if (i + 1) % 50 == 0:
            time.sleep(1)
    
    all_stocks.sort(key=lambda x: x['pct_from_wma'])
    summary = generate_landing_page_data(all_stocks)
    
    output = {
        'summary': summary,
        'stocks': all_stocks
    }
    
    output_file = OUTPUT_DIR / 'stocks.json'
    with open(output_file, 'w') as f:
        json.dump(output, f, separators=(',', ':'), cls=NumpyEncoder)
    
    print("\n" + "=" * 60)
    print("Pipeline Complete!")
    print(f"Processed: {len(all_stocks)} stocks")
    print(f"Errors: {len(errors)} stocks")
    print(f"Below line: {summary['below_line_count']}")
    print(f"Cannibals (buybacks + below): {summary['cannibal_count']}")
    print(f"Buffett quality (below): {summary['buffett_count']}")
    print(f"Aristocrats (below): {summary['aristocrat_count']}")
    print(f"Diluting stocks: {summary['diluting_count']}")
    print(f"Output: {output_file}")
    if errors:
        print(f"Failed symbols: {', '.join(errors[:20])}")
    print("=" * 60)


if __name__ == '__main__':
    main()
