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
    'ACIW',
    'ACLS',
    'ACLX',
    'ACM',
    'ACMR',
    'ACN',
    'ACVA',
    'ADBE',
    'ADC',
    'ADEA',
    'ADI',
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
    'AEM',
    'AEO',
    'AEP',
    'AES',
    'AFG',
    'AFL',
    'AFRM',
    'AG',
    'AGCO',
    'AGNC',
    'AGO',
    'AHCO',
    'AHH',
    'AI',
    'AIG',
    'AIT',
    'AIV',
    'AIZ',
    'AJG',
    'AKAM',
    'AKR',
    'AKRO',
    'ALB',
    'ALE',
    'ALEC',
    'ALEX',
    'ALGM',
    'ALGN',
    'ALGT',
    'ALHC',
    'ALIT',
    'ALK',
    'ALKS',
    'ALKT',
    'ALL',
    'ALLE',
    'ALLO',
    'ALLY',
    'ALNY',
    'ALRM',
    'ALRS',
    'ALTR',
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
    'AMR',
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
    'ANGI',
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
    'APG',
    'APH',
    'APLD',
    'APLE',
    'APLS',
    'APOG',
    'APP',
    'APPF',
    'APPN',
    'APPS',
    'APTV',
    'AQN',
    'AR',
    'ARAY',
    'ARCB',
    'ARCC',
    'ARCH',
    'ARCT',
    'ARDX',
    'ARE',
    'ARES',
    'ARGX',
    'ARHS',
    'ARIS',
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
    'ASAN',
    'ASB',
    'ASGN',
    'ASIX',
    'ASLE',
    'ASML',
    'ASPI',
    'ASPN',
    'ASTE',
    'ASTS',
    'ATEC',
    'ATEN',
    'ATHM',
    'ATI',
    'ATKR',
    'ATNI',
    'ATO',
    'ATRA',
    'ATRC',
    'ATRO',
    'AUB',
    'AUR',
    'AVA',
    'AVAH',
    'AVAV',
    'AVB',
    'AVGO',
    'AVNS',
    'AVNT',
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
    'AZEK',
    'AZN',
    'AZO',
    'AZTA',
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
    'BARK',
    'BAX',
    'BB',
    'BBAI',
    'BBY',
    'BC',
    'BCC',
    'BCE',
    'BCML',
    'BCO',
    'BCPC',
    'BCRX',
    'BCSF',
    'BDC',
    'BDX',
    'BE',
    'BEAM',
    'BECN',
    'BEN',
    'BEP',
    'BF-B',
    'BFAM',
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
    'BIRK',
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
    'BLD',
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
    'BRBR',
    'BRC',
    'BRCC',
    'BRK-B',
    'BRKR',
    'BRO',
    'BROS',
    'BRSP',
    'BRX',
    'BRY',
    'BRZE',
    'BSM',
    'BSRR',
    'BSX',
    'BTAI',
    'BTBT',
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
    'BXSL',
    'BY',
    'BYD',
    'BYND',
    'BYON',
    'C',
    'CAC',
    'CACC',
    'CACI',
    'CADE',
    'CAG',
    'CAH',
    'CAKE',
    'CALM',
    'CALX',
    'CAMP',
    'CAMT',
    'CAR',
    'CARG',
    'CARR',
    'CASS',
    'CAT',
    'CATC',
    'CATO',
    'CATY',
    'CAVA',
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
    'CC',
    'CCBG',
    'CCI',
    'CCJ',
    'CCL',
    'CCNE',
    'CCOI',
    'CCS',
    'CCSI',
    'CDE',
    'CDNA',
    'CDNS',
    'CDP',
    'CDRE',
    'CDW',
    'CDXS',
    'CE',
    'CECO',
    'CEG',
    'CEIX',
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
    'CLBT',
    'CLDT',
    'CLF',
    'CLFD',
    'CLH',
    'CLNE',
    'CLOV',
    'CLPR',
    'CLSK',
    'CLVT',
    'CLW',
    'CLX',
    'CMA',
    'CMC',
    'CMCO',
    'CMCSA',
    'CME',
    'CMG',
    'CMI',
    'CMPR',
    'CMS',
    'CMT',
    'CNA',
    'CNC',
    'CNDT',
    'CNK',
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
    'COLD',
    'COLL',
    'COLM',
    'COMM',
    'COMP',
    'COO',
    'COOP',
    'COP',
    'COR',
    'CORT',
    'CORZ',
    'COST',
    'COTY',
    'COUR',
    'CPAY',
    'CPB',
    'CPK',
    'CPNG',
    'CPRI',
    'CPRT',
    'CPRX',
    'CPT',
    'CQP',
    'CR',
    'CRBG',
    'CRC',
    'CRGY',
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
    'CRSP',
    'CRTO',
    'CRUS',
    'CRVL',
    'CRWD',
    'CSCO',
    'CSGP',
    'CSGS',
    'CSIQ',
    'CSTL',
    'CSTM',
    'CSV',
    'CSWC',
    'CSWI',
    'CSX',
    'CTAS',
    'CTLT',
    'CTO',
    'CTRA',
    'CTSH',
    'CTV',
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
    'CWAN',
    'CWEN',
    'CWH',
    'CWST',
    'CXM',
    'CXT',
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
    'DASH',
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
    'DEA',
    'DECK',
    'DEI',
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
    'DINE',
    'DINO',
    'DIOD',
    'DIS',
    'DJT',
    'DKNG',
    'DKS',
    'DLB',
    'DLHC',
    'DLO',
    'DLR',
    'DLTR',
    'DLX',
    'DMLP',
    'DNA',
    'DNLI',
    'DNN',
    'DNOW',
    'DNUT',
    'DOC',
    'DOCN',
    'DOCS',
    'DOCU',
    'DOMO',
    'DORM',
    'DOV',
    'DOW',
    'DOX',
    'DPZ',
    'DQ',
    'DRH',
    'DRI',
    'DRVN',
    'DSGX',
    'DT',
    'DTE',
    'DTM',
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
    'EDIT',
    'EDR',
    'EDU',
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
    'ELF',
    'ELME',
    'ELS',
    'ELV',
    'EMBC',
    'EME',
    'EMN',
    'EMR',
    'ENB',
    'ENOV',
    'ENPH',
    'ENS',
    'ENSG',
    'ENTA',
    'ENTG',
    'ENVA',
    'ENVX',
    'EOG',
    'EPAC',
    'EPAM',
    'EPD',
    'EPM',
    'EPRT',
    'EQBK',
    'EQH',
    'EQIX',
    'EQNR',
    'EQR',
    'EQT',
    'ERIE',
    'ERII',
    'ES',
    'ESCA',
    'ESE',
    'ESGR',
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
    'EU',
    'EVC',
    'EVCM',
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
    'FG',
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
    'FLNC',
    'FLO',
    'FLR',
    'FLS',
    'FLWS',
    'FMBH',
    'FMC',
    'FMX',
    'FN',
    'FNB',
    'FND',
    'FNF',
    'FNV',
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
    'FSK',
    'FSLR',
    'FSLY',
    'FTI',
    'FTNT',
    'FTV',
    'FUBO',
    'FUL',
    'FUTU',
    'FWONA',
    'FWONK',
    'FWRD',
    'FWRG',
    'G',
    'GABC',
    'GAIN',
    'GATX',
    'GBCI',
    'GBDC',
    'GBTC',
    'GBX',
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
    'GH',
    'GHC',
    'GIII',
    'GIL',
    'GILD',
    'GIS',
    'GKOS',
    'GL',
    'GLAD',
    'GLBE',
    'GLDD',
    'GLOB',
    'GLW',
    'GM',
    'GMAB',
    'GME',
    'GMED',
    'GMRE',
    'GNRC',
    'GNTX',
    'GNW',
    'GO',
    'GOEV',
    'GOGO',
    'GOLD',
    'GOLF',
    'GOOD',
    'GOOG',
    'GOOGL',
    'GOOS',
    'GPC',
    'GPI',
    'GPK',
    'GPMT',
    'GPN',
    'GPOR',
    'GPRE',
    'GPRO',
    'GPS',
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
    'GSBD',
    'GSK',
    'GTBIF',
    'GTES',
    'GTLB',
    'GTLS',
    'GTN',
    'GTY',
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
    'HAYN',
    'HAYW',
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
    'HEXO',
    'HGV',
    'HI',
    'HIFS',
    'HIG',
    'HII',
    'HIMS',
    'HIW',
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
    'HOG',
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
    'IDA',
    'IDCC',
    'IDT',
    'IDXX',
    'IESC',
    'IEX',
    'IFF',
    'IHG',
    'IHRT',
    'IIIV',
    'IIPR',
    'ILMN',
    'ILPT',
    'IMAX',
    'IMMR',
    'IMVT',
    'INBK',
    'INCY',
    'INDB',
    'INFU',
    'INFY',
    'ING',
    'INGR',
    'INMD',
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
    'IOSP',
    'IOT',
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
    'IREN',
    'IRM',
    'IRMD',
    'IRT',
    'IRTC',
    'IRWD',
    'ISRG',
    'IT',
    'ITCI',
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
    'KD',
    'KDP',
    'KE',
    'KEX',
    'KEY',
    'KEYS',
    'KFRC',
    'KFY',
    'KGC',
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
    'KNTK',
    'KNX',
    'KO',
    'KOD',
    'KOS',
    'KR',
    'KRC',
    'KREF',
    'KRG',
    'KRNT',
    'KROS',
    'KRP',
    'KRUS',
    'KRYS',
    'KSS',
    'KTB',
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
    'LANC',
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
    'LEGN',
    'LEN',
    'LESL',
    'LEU',
    'LEVI',
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
    'LSXMA',
    'LSXMK',
    'LTBR',
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
    'LYFT',
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
    'MERC',
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
    'MGY',
    'MHK',
    'MHO',
    'MIDD',
    'MIND',
    'MIR',
    'MIRM',
    'MITK',
    'MKC',
    'MKL',
    'MKSI',
    'MKTX',
    'MLCO',
    'MLI',
    'MLM',
    'MLR',
    'MMC',
    'MMM',
    'MMSI',
    'MNDY',
    'MNKD',
    'MNR',
    'MNRO',
    'MNSO',
    'MNST',
    'MNTK',
    'MNTS',
    'MO',
    'MOD',
    'MODG',
    'MOFG',
    'MOG-A',
    'MOH',
    'MOS',
    'MOV',
    'MP',
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
    'MTZ',
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
    'NKLA',
    'NL',
    'NLY',
    'NMFC',
    'NMIH',
    'NMM',
    'NMRA',
    'NMRK',
    'NNE',
    'NNI',
    'NNN',
    'NNOX',
    'NOC',
    'NODK',
    'NOG',
    'NOK',
    'NOV',
    'NOVA',
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
    'NXE',
    'NXPI',
    'NXRT',
    'NXST',
    'NXTC',
    'NYT',
    'O',
    'OBDC',
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
    'OKLO',
    'OKTA',
    'OLINK',
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
    'ONON',
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
    'ORCC',
    'ORCL',
    'ORGO',
    'ORI',
    'ORIC',
    'ORLA',
    'ORLY',
    'ORN',
    'ORRF',
    'OSCR',
    'OSH',
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
    'PAY',
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
    'PDM',
    'PEG',
    'PEGA',
    'PENN',
    'PEP',
    'PESI',
    'PFE',
    'PFG',
    'PFGC',
    'PFSI',
    'PG',
    'PGNY',
    'PGR',
    'PH',
    'PHM',
    'PII',
    'PINS',
    'PIPR',
    'PJT',
    'PKG',
    'PL',
    'PLAY',
    'PLD',
    'PLMR',
    'PLNT',
    'PLTR',
    'PLUG',
    'PLUS',
    'PLYM',
    'PM',
    'PNC',
    'PNFP',
    'PNM',
    'PNR',
    'PNW',
    'PODD',
    'POOL',
    'POR',
    'POST',
    'POWI',
    'POWL',
    'PPC',
    'PPG',
    'PPL',
    'PR',
    'PRAA',
    'PRCT',
    'PRGO',
    'PRGS',
    'PRI',
    'PRIM',
    'PRPL',
    'PRTA',
    'PRU',
    'PSA',
    'PSFE',
    'PSN',
    'PSTG',
    'PSX',
    'PTC',
    'PTEN',
    'PTON',
    'PUBM',
    'PUMP',
    'PVH',
    'PWR',
    'PXD',
    'PYPL',
    'QBTS',
    'QCOM',
    'QLYS',
    'QRVO',
    'QS',
    'QUBT',
    'RACE',
    'RAMP',
    'RARE',
    'RBC',
    'RBLX',
    'RCAT',
    'RCKT',
    'RCL',
    'RCUS',
    'RDDT',
    'RDFN',
    'RDN',
    'RDW',
    'REG',
    'REGN',
    'RELY',
    'REPX',
    'REXR',
    'RF',
    'RGLD',
    'RGNX',
    'RGTI',
    'RH',
    'RHP',
    'RIG',
    'RIO',
    'RIOT',
    'RITM',
    'RIVN',
    'RJF',
    'RKLB',
    'RKT',
    'RL',
    'RLI',
    'RMD',
    'RNG',
    'RNR',
    'ROAD',
    'ROK',
    'ROKU',
    'ROL',
    'ROOT',
    'ROP',
    'ROST',
    'RPD',
    'RPM',
    'RPRX',
    'RRC',
    'RRR',
    'RRX',
    'RS',
    'RSG',
    'RSI',
    'RTX',
    'RUN',
    'RVLV',
    'RVMD',
    'RXO',
    'RXRX',
    'RYAN',
    'S',
    'SAIA',
    'SAIC',
    'SAM',
    'SAN',
    'SAP',
    'SARO',
    'SATS',
    'SAVA',
    'SBAC',
    'SBCF',
    'SBRA',
    'SBUX',
    'SCHD',
    'SCHW',
    'SE',
    'SEAS',
    'SEDG',
    'SEE',
    'SEIC',
    'SF',
    'SFM',
    'SFNC',
    'SHAK',
    'SHEL',
    'SHG',
    'SHLS',
    'SHOO',
    'SHOP',
    'SHW',
    'SIRI',
    'SITE',
    'SIX',
    'SJM',
    'SKLZ',
    'SKX',
    'SLB',
    'SLDP',
    'SLM',
    'SLRC',
    'SLVM',
    'SM',
    'SMCI',
    'SMFG',
    'SMMT',
    'SMPL',
    'SMR',
    'SMTC',
    'SNA',
    'SNAP',
    'SNDL',
    'SNDR',
    'SNEX',
    'SNOW',
    'SNPS',
    'SNV',
    'SNY',
    'SO',
    'SOFI',
    'SOLV',
    'SON',
    'SONY',
    'SOUN',
    'SPB',
    'SPCE',
    'SPG',
    'SPGI',
    'SPOT',
    'SPSC',
    'SPXC',
    'SQ',
    'SR',
    'SRE',
    'SRPT',
    'SSB',
    'SSNC',
    'SSP',
    'STAG',
    'STE',
    'STEM',
    'STEP',
    'STLA',
    'STLD',
    'STRL',
    'STT',
    'STWD',
    'STX',
    'STZ',
    'SUI',
    'SWK',
    'SWKS',
    'SWN',
    'SWTX',
    'SWX',
    'SXT',
    'SYF',
    'SYK',
    'SYM',
    'SYNA',
    'SYY',
    'T',
    'TAL',
    'TALO',
    'TAP',
    'TASK',
    'TCBI',
    'TCOM',
    'TDG',
    'TDW',
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
    'TGI',
    'TGNA',
    'TGT',
    'TGTX',
    'THS',
    'TIGR',
    'TJX',
    'TLN',
    'TLRY',
    'TM',
    'TMDX',
    'TME',
    'TMO',
    'TMUS',
    'TNDM',
    'TNET',
    'TOL',
    'TOST',
    'TPC',
    'TPG',
    'TPL',
    'TPR',
    'TPVG',
    'TREE',
    'TREX',
    'TRGP',
    'TRIP',
    'TRMB',
    'TRN',
    'TROW',
    'TROX',
    'TRV',
    'TSCO',
    'TSLA',
    'TSLX',
    'TSM',
    'TSN',
    'TT',
    'TTC',
    'TTD',
    'TTE',
    'TTGT',
    'TTWO',
    'TW',
    'TWLO',
    'TWO',
    'TWST',
    'TXN',
    'TXRH',
    'TXT',
    'TYL',
    'U',
    'UAL',
    'UBER',
    'UBSI',
    'UDR',
    'UEC',
    'UFPI',
    'UHS',
    'UL',
    'ULTA',
    'UMH',
    'UNH',
    'UNIT',
    'UNP',
    'UPS',
    'UPST',
    'URBN',
    'URG',
    'URI',
    'USB',
    'USFD',
    'USNA',
    'UTHR',
    'UTL',
    'UUUU',
    'UWMC',
    'V',
    'VALE',
    'VEEV',
    'VERA',
    'VERX',
    'VFC',
    'VFF',
    'VICI',
    'VIRT',
    'VIST',
    'VKTX',
    'VLO',
    'VLTO',
    'VMC',
    'VNDA',
    'VNO',
    'VNOM',
    'VOYA',
    'VRNS',
    'VRSK',
    'VRSN',
    'VRT',
    'VRTX',
    'VSCO',
    'VST',
    'VTLE',
    'VTOL',
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
    'WD',
    'WDAY',
    'WDC',
    'WDFC',
    'WEC',
    'WELL',
    'WEN',
    'WERN',
    'WEX',
    'WFC',
    'WFRD',
    'WH',
    'WHD',
    'WING',
    'WIRE',
    'WIT',
    'WK',
    'WKHS',
    'WLK',
    'WLY',
    'WM',
    'WMB',
    'WMG',
    'WMK',
    'WMS',
    'WMT',
    'WOLF',
    'WPM',
    'WRB',
    'WRBY',
    'WRK',
    'WSC',
    'WSM',
    'WST',
    'WTFC',
    'WTM',
    'WTRG',
    'WTS',
    'WTW',
    'WU',
    'WULF',
    'WWD',
    'WY',
    'WYNN',
    'XEL',
    'XENE',
    'XNCR',
    'XOM',
    'XPEV',
    'XPO',
    'XPOF',
    'XRAY',
    'XYL',
    'Y',
    'YELP',
    'YETI',
    'YMM',
    'YOU',
    'YUM',
    'Z',
    'ZBH',
    'ZBRA',
    'ZETA',
    'ZG',
    'ZI',
    'ZION',
    'ZM',
    'ZS',
    'ZTO',
    'ZTS',
    'ZUO',
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


def get_fcf_trend(ticker) -> dict:
    """Calculate free cash flow trend from annual cashflow statements.
    
    Returns:
        dict with fcf_trend ('growing'|'declining'|'volatile'|'insufficient_data'),
        fcf_cagr_3yr (compound annual growth rate), fcf_consecutive_positive (years),
        and fcf_history (list of {year, fcf} dicts).
    """
    empty = {
        'fcf_trend': 'insufficient_data',
        'fcf_cagr_3yr': None,
        'fcf_consecutive_positive': 0,
        'fcf_history': [],
    }
    try:
        cf = ticker.cashflow
        if cf is None or cf.empty:
            return empty
        
        # yfinance cashflow: rows are line items, columns are fiscal year dates
        # Look for Free Cash Flow row
        fcf_row = None
        for label in ['Free Cash Flow', 'FreeCashFlow']:
            if label in cf.index:
                fcf_row = cf.loc[label]
                break
        
        if fcf_row is None:
            return empty
        
        # Sort by date ascending (oldest first), drop NaN
        fcf_row = fcf_row.dropna().sort_index(ascending=True)
        
        if len(fcf_row) < 3:
            return empty
        
        # Build history (most recent 4 years max)
        fcf_values = fcf_row.tail(4)
        history = []
        for date, val in fcf_values.items():
            year = date.year if hasattr(date, 'year') else pd.Timestamp(date).year
            history.append({'year': year, 'fcf': round(float(val))})
        
        # Consecutive positive years (counting from most recent backward)
        consecutive_positive = 0
        for entry in reversed(history):
            if entry['fcf'] > 0:
                consecutive_positive += 1
            else:
                break
        
        # 3-year CAGR: use oldest and newest of the last 4 data points
        cagr = None
        if len(history) >= 3:
            oldest_fcf = float(fcf_values.iloc[0])
            newest_fcf = float(fcf_values.iloc[-1])
            years_span = len(fcf_values) - 1  # gaps between points
            if oldest_fcf > 0 and newest_fcf > 0 and years_span > 0:
                cagr = round(((newest_fcf / oldest_fcf) ** (1 / years_span) - 1) * 100, 1)
            elif oldest_fcf > 0 and newest_fcf <= 0:
                cagr = -100.0  # went from positive to negative
        
        # Trend classification using year-over-year changes
        yoy_changes = []
        vals = list(fcf_values)
        for i in range(1, len(vals)):
            prev = float(vals[i-1])
            curr = float(vals[i])
            if prev != 0:
                yoy_changes.append(curr > prev)
            else:
                yoy_changes.append(curr > 0)
        
        ups = sum(yoy_changes)
        downs = len(yoy_changes) - ups
        
        if ups >= 2 and (cagr is not None and cagr > 0):
            trend = 'growing'
        elif downs >= 2 and (cagr is None or cagr < 0):
            trend = 'declining'
        else:
            trend = 'volatile'
        
        return {
            'fcf_trend': trend,
            'fcf_cagr_3yr': cagr,
            'fcf_consecutive_positive': consecutive_positive,
            'fcf_history': history,
        }
    except Exception:
        return empty


def fetch_fundamental_data(symbol: str) -> dict:
    """
    Fetch fundamental data for quality screening.
    
    Includes:
    - Yartseva multibagger metrics (FCF yield, P/B, market cap)
    - Buffett quality metrics (ROE, debt/equity, margins)
    - Share buyback/dilution tracking
    - Dividend info
    - Free cash flow trend
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
        
        # Free cash flow trend
        fcf_trend_data = get_fcf_trend(ticker)
        
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
            # FCF trend
            'fcf_trend': fcf_trend_data['fcf_trend'],
            'fcf_cagr_3yr': fcf_trend_data['fcf_cagr_3yr'],
            'fcf_consecutive_positive': fcf_trend_data['fcf_consecutive_positive'],
            'fcf_history': fcf_trend_data['fcf_history'],
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
            'fcf_trend': 'insufficient_data', 'fcf_cagr_3yr': None,
            'fcf_consecutive_positive': 0, 'fcf_history': [],
        }


def fetch_insider_data(symbol: str) -> dict:
    """
    Fetch insider buying data from SEC Form 4 filings via yfinance.
    
    Filters for open-market purchases that signal conviction:
    - Transaction value >= $500K
    - Position increase >= 10%
    - Only direct purchases (not grants, awards, or option exercises)
    
    Also detects cluster buys: 3+ insiders purchasing within 30 days.
    """
    empty_result = {
        'insider_buys': [],
        'has_conviction_buy': False,
        'has_cluster_buy': False,
        'largest_buy_value': None,
        'insider_buy_count_12m': 0,
        'insider_buy_total_12m': 0,
    }
    
    try:
        ticker = yf.Ticker(symbol)
        
        # Get transaction history
        transactions = ticker.insider_transactions
        if transactions is None or transactions.empty:
            return empty_result
        
        # Get roster for total position sizes (to calculate % increase)
        roster = None
        try:
            roster = ticker.insider_roster_holders
        except Exception:
            pass
        
        # Build lookup of total shares owned by insider name
        position_lookup = {}
        if roster is not None and not roster.empty:
            for _, row in roster.iterrows():
                name = str(row.get('Name', '')).strip().upper()
                direct = row.get('Shares Owned Directly')
                indirect = row.get('Shares Owned Indirectly')
                total = 0
                if pd.notna(direct):
                    total += float(direct)
                if pd.notna(indirect):
                    total += float(indirect)
                if total > 0:
                    position_lookup[name] = total
        
        # Filter for open-market purchases only
        cutoff_12m = pd.Timestamp.now() - pd.Timedelta(days=365)
        conviction_buys = []
        all_purchase_dates = []
        total_buy_value = 0
        
        for _, row in transactions.iterrows():
            text = str(row.get('Text', ''))
            
            # Only open-market purchases — skip grants, awards, options, sales
            if 'Purchase' not in text:
                continue
            
            value = row.get('Value')
            if pd.isna(value) or value is None:
                continue
            value = float(value)
            
            shares = row.get('Shares')
            if pd.isna(shares) or shares is None:
                continue
            shares = float(shares)
            
            date_raw = row.get('Start Date')
            if pd.isna(date_raw):
                continue
            tx_date = pd.Timestamp(date_raw)
            
            # Only last 12 months
            if tx_date < cutoff_12m:
                continue
            
            insider_name = str(row.get('Insider', 'Unknown')).strip()
            position = str(row.get('Position', 'Unknown')).strip()
            
            # Calculate position increase %
            pct_increase = None
            name_upper = insider_name.upper()
            if name_upper in position_lookup:
                total_owned = position_lookup[name_upper]
                prior_shares = total_owned - shares
                if prior_shares > 0:
                    pct_increase = round((shares / prior_shares) * 100, 1)
            
            all_purchase_dates.append(tx_date)
            total_buy_value += value
            
            # Conviction buy logic:
            # - $2M+ purchase = always conviction (large holders may have small % increase)
            # - $500K-$2M purchase = conviction only if >= 10% position increase or unknown
            is_conviction = (
                value >= 2_000_000 or
                (value >= 500_000 and (pct_increase is None or pct_increase >= 10))
            )
            
            if is_conviction:
                conviction_buys.append({
                    'name': insider_name,
                    'title': position,
                    'date': tx_date.strftime('%Y-%m-%d'),
                    'shares': int(shares),
                    'value': round(value),
                    'pct_position_increase': pct_increase,
                })
        
        # Cluster buy detection: 3+ separate insiders buying within any 30-day window
        has_cluster = False
        if len(all_purchase_dates) >= 3:
            sorted_dates = sorted(all_purchase_dates)
            for i in range(len(sorted_dates) - 2):
                window = sorted_dates[i] + pd.Timedelta(days=30)
                buys_in_window = sum(1 for d in sorted_dates if sorted_dates[i] <= d <= window)
                if buys_in_window >= 3:
                    has_cluster = True
                    break
        
        # Sort conviction buys by date (most recent first)
        conviction_buys.sort(key=lambda x: x['date'], reverse=True)
        
        largest = max((b['value'] for b in conviction_buys), default=None)
        
        return {
            'insider_buys': conviction_buys,
            'has_conviction_buy': len(conviction_buys) > 0,
            'has_cluster_buy': has_cluster,
            'largest_buy_value': largest,
            'insider_buy_count_12m': len(all_purchase_dates),
            'insider_buy_total_12m': round(total_buy_value),
        }
        
    except Exception as e:
        return empty_result


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


def find_historical_touches(df: pd.DataFrame, recovery_weeks: int = 2) -> List[dict]:
    """Find historical episodes where price spent time below 200WMA.
    
    Uses hysteresis to avoid noise: a stock must stay ABOVE the 200WMA
    for at least `recovery_weeks` consecutive weeks before we consider
    the episode over. Brief 1-week bounces above the line get merged
    into the same episode.
    
    Each touch records:
    - date: when the episode started (first week below)
    - recovery_date: when the episode ended (first sustained week above)
    - weeks_below: total weeks in the episode (including brief bounces)
    - max_depth: deepest % below the 200WMA during the episode
    - return_1yr: price return 1 year after the episode started
    - ongoing: True if the stock is still in this episode
    """
    df = df.copy()
    df['below'] = df['adjusted_close'] < df['WMA_200']
    
    touches = []
    i = 0
    n = len(df)
    
    while i < n:
        # Scan forward until we find a week that's below the line
        if not df.iloc[i]['below']:
            i += 1
            continue
        
        # Found the start of an episode
        episode_start = i
        
        # Now scan forward to find the end of the episode.
        # The episode ends when we see `recovery_weeks` consecutive
        # weeks ABOVE the line. Until then, brief bounces above
        # are still part of this episode.
        j = i + 1
        consecutive_above = 0
        episode_end = None  # will be set to first week of the sustained recovery
        
        while j < n:
            if not df.iloc[j]['below']:
                consecutive_above += 1
                if consecutive_above >= recovery_weeks:
                    # Recovery is real — episode ended at start of this above-streak
                    episode_end = j - consecutive_above + 1
                    break
            else:
                consecutive_above = 0
            j += 1
        
        # Gather episode data
        start_date = df.index[episode_start]
        
        if episode_end is not None:
            # Completed episode
            episode_data = df.iloc[episode_start:episode_end]
            weeks_below = len(episode_data)
            min_pct = episode_data['pct_from_wma'].min()
            max_depth = abs(min_pct)
            recovery_date = df.index[episode_end].strftime('%b %Y')
            
            # 1-year return from episode start
            one_year_later_idx = episode_start + 52
            if one_year_later_idx < n:
                entry_price = df.iloc[episode_start]['adjusted_close']
                exit_price = df.iloc[one_year_later_idx]['adjusted_close']
                return_1yr = ((exit_price - entry_price) / entry_price) * 100
            else:
                return_1yr = None
            
            touches.append({
                'date': start_date.strftime('%b %Y'),
                'date_iso': start_date.strftime('%Y-%m-%d'),
                'recovery_date': recovery_date,
                'weeks_below': int(weeks_below),
                'max_depth': round(float(max_depth), 1),
                'return_1yr': round(float(return_1yr), 1) if return_1yr is not None else None,
                'ongoing': False
            })
            
            # Jump past the episode
            i = episode_end
        else:
            # Ongoing episode (hasn't recovered yet)
            episode_data = df.iloc[episode_start:]
            weeks_below = len(episode_data)
            min_pct = episode_data['pct_from_wma'].min()
            max_depth = abs(min_pct)
            
            touches.append({
                'date': start_date.strftime('%b %Y'),
                'date_iso': start_date.strftime('%Y-%m-%d'),
                'recovery_date': None,
                'weeks_below': int(weeks_below),
                'max_depth': round(float(max_depth), 1),
                'return_1yr': None,
                'ongoing': True
            })
            
            # We've reached the end of the data
            break
    
    return touches


def fetch_spy_monthly() -> pd.Series:
    """Fetch SPY monthly close data once for benchmark comparisons."""
    print("Fetching SPY benchmark data...")
    spy = yf.Ticker('SPY')
    df = spy.history(period='max', interval='1mo')
    if df.empty:
        print("  ✗ Could not fetch SPY data")
        return pd.Series(dtype=float)
    series = df['Close'].dropna()
    series.index = series.index.to_period('M')
    print(f"  ✓ SPY: {len(series)} months of data")
    return series


def build_growth_chart(df: pd.DataFrame, spy_monthly: pd.Series,
                       touches: List[dict]) -> Optional[dict]:
    """Build normalized $100 growth chart data for stock vs SPY.
    
    Returns compact dict:
      start: "YYYY-MM" first month
      s: [100, 103, ...] stock growth, integers
      b: [100, 101, ...] SPY benchmark growth, integers  
      t: [36, 182, ...]  monthly indices where touches occurred
    """
    try:
        # Resample stock to monthly (last close per month)
        monthly = df['adjusted_close'].resample('MS').last().dropna()
        if len(monthly) < 12:
            return None
        
        monthly.index = monthly.index.to_period('M')
        
        # Find overlapping date range with SPY
        common_start = max(monthly.index.min(), spy_monthly.index.min())
        common_end = min(monthly.index.max(), spy_monthly.index.max())
        
        stock_aligned = monthly[(monthly.index >= common_start) & (monthly.index <= common_end)]
        spy_aligned = spy_monthly[(spy_monthly.index >= common_start) & (spy_monthly.index <= common_end)]
        
        # Use only months present in both
        common_periods = stock_aligned.index.intersection(spy_aligned.index)
        if len(common_periods) < 12:
            return None
        
        stock_aligned = stock_aligned[common_periods]
        spy_aligned = spy_aligned[common_periods]
        
        # Normalize to $100
        stock_norm = (stock_aligned / stock_aligned.iloc[0]) * 100
        spy_norm = (spy_aligned / spy_aligned.iloc[0]) * 100
        
        # Round to integers for compact JSON
        stock_values = [int(round(v)) for v in stock_norm.values]
        spy_values = [int(round(v)) for v in spy_norm.values]
        
        # Map touch dates to monthly indices
        touch_indices = []
        period_list = list(common_periods)
        for touch in touches:
            try:
                touch_period = pd.Period(touch['date_iso'][:7], freq='M')
                # Find closest month
                for idx, p in enumerate(period_list):
                    if p >= touch_period:
                        touch_indices.append(idx)
                        break
            except (ValueError, KeyError):
                continue
        
        # Total return comparison for prose
        stock_total_return = round(((stock_norm.iloc[-1] / 100) - 1) * 100, 1)
        spy_total_return = round(((spy_norm.iloc[-1] / 100) - 1) * 100, 1)
        years = len(common_periods) / 12
        
        # Annualized returns
        stock_annual = round(((stock_norm.iloc[-1] / 100) ** (1 / years) - 1) * 100, 1) if years > 0 else None
        spy_annual = round(((spy_norm.iloc[-1] / 100) ** (1 / years) - 1) * 100, 1) if years > 0 else None
        
        return {
            'start': str(common_periods[0]),
            's': stock_values,
            'b': spy_values,
            't': touch_indices,
            'stock_total_return': stock_total_return,
            'spy_total_return': spy_total_return,
            'stock_annual_return': stock_annual,
            'spy_annual_return': spy_annual,
            'years': round(years, 1),
            'beats_spy': stock_total_return > spy_total_return
        }
    except Exception as e:
        print(f"    ⚠ Growth chart error: {e}")
        return None


def build_touch_overlay_chart(df: pd.DataFrame, spy_monthly: pd.Series,
                               touches: List[dict], months: int = 24,
                               max_chart_episodes: int = 10) -> Optional[dict]:
    """Build overlaid touch chart: what happened after each 200WMA crossing.
    
    Each touch is normalized to $100 at crossing date.
    X-axis = months since crossing (0 to 24).
    Shows stock line per episode + average stock line + average SPY line.
    
    Returns compact dict:
      episodes: [{date, s: [100,95,...], months_available}]  (capped at 10 most recent)
      stock_avg: [100, 97, ...]    average stock across ALL episodes
      spy_avg:   [100, 101, ...]   average SPY across ALL episodes
      total_episodes: N
      episodes_shown: min(N, 10)
      avg_return_12m, median_return_12m, pct_positive_12m
      avg_return_24m, median_return_24m, pct_positive_24m
    """
    try:
        # Resample stock to monthly
        monthly = df['adjusted_close'].resample('MS').last().dropna()
        if len(monthly) < 12:
            return None
        monthly.index = monthly.index.to_period('M')
        
        episodes = []
        all_stock_arrays = []
        all_spy_arrays = []
        
        for touch in touches:
            try:
                start_period = pd.Period(touch['date_iso'][:7], freq='M')
            except (ValueError, KeyError):
                continue
            
            # Get up to months+1 data points starting from touch month
            stock_slice = monthly[monthly.index >= start_period].head(months + 1)
            spy_slice = spy_monthly[spy_monthly.index >= start_period].head(months + 1)
            
            if len(stock_slice) < 3 or len(spy_slice) < 3:
                continue
            
            # Align to common periods
            common = stock_slice.index.intersection(spy_slice.index)
            if len(common) < 3:
                continue
            
            stock_vals = stock_slice[common]
            spy_vals = spy_slice[common]
            
            # Normalize to $100
            stock_norm = [int(round((v / stock_vals.iloc[0]) * 100))
                          for v in stock_vals.values]
            spy_norm = [int(round((v / spy_vals.iloc[0]) * 100))
                        for v in spy_vals.values]
            
            ep = {
                'date': touch['date'],
                's': stock_norm,
                'months': len(stock_norm) - 1,
            }
            episodes.append(ep)
            all_stock_arrays.append(stock_norm)
            all_spy_arrays.append(spy_norm)
        
        if not episodes:
            return None
        
        # Average lines across all episodes (handles varying lengths)
        max_len = min(months + 1, max(len(a) for a in all_stock_arrays))
        
        def avg_line(arrays, length):
            result = []
            for i in range(length):
                vals = [a[i] for a in arrays if i < len(a)]
                if vals:
                    result.append(int(round(sum(vals) / len(vals))))
            return result
        
        stock_avg = avg_line(all_stock_arrays, max_len)
        spy_avg = avg_line(all_spy_arrays, max_len)
        
        # Stats at 12-month and 24-month marks across ALL episodes
        def get_returns_at(arrays, month_idx):
            """Get return (value - 100) at a given month index."""
            return [a[month_idx] - 100 for a in arrays if len(a) > month_idx]
        
        returns_12m = get_returns_at(all_stock_arrays, 12)
        returns_24m = get_returns_at(all_stock_arrays, 24)
        spy_returns_12m = get_returns_at(all_spy_arrays, 12)
        spy_returns_24m = get_returns_at(all_spy_arrays, 24)
        
        def stat_block(returns):
            if not returns:
                return {'avg': None, 'median': None, 'pct_positive': None}
            s = sorted(returns)
            return {
                'avg': round(sum(s) / len(s), 1),
                'median': round(s[len(s) // 2], 1),
                'pct_positive': round(sum(1 for r in s if r > 0) / len(s) * 100, 0),
            }
        
        stats_12m = stat_block(returns_12m)
        stats_24m = stat_block(returns_24m)
        spy_stats_12m = stat_block(spy_returns_12m)
        spy_stats_24m = stat_block(spy_returns_24m)
        
        # Cap chart episodes to most recent N
        shown = episodes[-max_chart_episodes:] if len(episodes) > max_chart_episodes else episodes
        
        return {
            'episodes': shown,
            'stock_avg': stock_avg,
            'spy_avg': spy_avg,
            'total_episodes': len(episodes),
            'episodes_shown': len(shown),
            # Stock stats
            'avg_return_12m': stats_12m['avg'],
            'median_return_12m': stats_12m['median'],
            'pct_positive_12m': stats_12m['pct_positive'],
            'avg_return_24m': stats_24m['avg'],
            'median_return_24m': stats_24m['median'],
            'pct_positive_24m': stats_24m['pct_positive'],
            # SPY comparison stats
            'spy_avg_return_12m': spy_stats_12m['avg'],
            'spy_avg_return_24m': spy_stats_24m['avg'],
        }
    except Exception as e:
        print(f"    \u26a0 Touch overlay error: {e}")
        return None


def calculate_stock_signals(symbol: str, spy_monthly: pd.Series = None) -> Optional[dict]:
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
    
    # Build growth chart vs SPY
    growth_chart = None
    touch_chart = None
    if spy_monthly is not None and not spy_monthly.empty:
        growth_chart = build_growth_chart(df_complete, spy_monthly, historical_touches)
        if historical_touches:
            touch_chart = build_touch_overlay_chart(df_complete, spy_monthly, historical_touches)
    
    # Add return_to_now for each historical touch
    current_price = float(df_complete.iloc[-1]['adjusted_close'])
    for touch in historical_touches:
        try:
            touch_date = pd.Timestamp(touch['date_iso'])
            # Match timezone of dataframe index (yfinance returns tz-aware)
            if df_complete.index.tz is not None and touch_date.tz is None:
                touch_date = touch_date.tz_localize(df_complete.index.tz)
            # Find the closest weekly row to the touch date
            mask = df_complete.index >= touch_date
            if mask.any():
                entry_price = float(df_complete.loc[mask].iloc[0]['adjusted_close'])
                touch['return_to_now'] = round(((current_price - entry_price) / entry_price) * 100, 1)
            else:
                touch['return_to_now'] = None
        except Exception:
            touch['return_to_now'] = None
    
    fundamentals = fetch_fundamental_data(symbol)
    insider = fetch_insider_data(symbol)
    
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
    insider_below_line = bool(insider['has_conviction_buy'] and below)
    fcf_growing_below_line = bool(fundamentals['fcf_trend'] == 'growing' and below)
    
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
        # Insider buying
        'insider_buys': insider['insider_buys'],
        'has_conviction_buy': insider['has_conviction_buy'],
        'has_cluster_buy': insider['has_cluster_buy'],
        'largest_buy_value': insider['largest_buy_value'],
        'insider_buy_count_12m': insider['insider_buy_count_12m'],
        'insider_buy_total_12m': insider['insider_buy_total_12m'],
        'insider_below_line': insider_below_line,
        # FCF trend
        'fcf_trend': fundamentals['fcf_trend'],
        'fcf_cagr_3yr': fundamentals['fcf_cagr_3yr'],
        'fcf_consecutive_positive': fundamentals['fcf_consecutive_positive'],
        'fcf_growing_below_line': fcf_growing_below_line,
        # Growth chart vs SPY
        'growth_chart': growth_chart,
        # Touch overlay chart
        'touch_chart': touch_chart,
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
    if insider_below_line: flags.append("🔍INSIDER")
    if fcf_growing_below_line: flags.append("📈FCF+")
    if fundamentals['is_diluting']: flags.append("⚠️DILUTE")
    flag_str = " " + " ".join(flags) if flags else ""
    
    print(f"  ✓ {symbol}: {pct:.1f}% from WMA, Zone: {zone}{flag_str}")
    return result


def _slim_stock(stock: dict) -> dict:
    """Strip heavy fields from a stock dict for summary lists.

    The homepage only reads a handful of fields (symbol, zone, pct_from_wma,
    approaching, rsi_14, avg_return_after_touch). Dropping the chart/history
    data from summary lists saves ~9 MB in stocks.json.
    """
    heavy_fields = {'growth_chart', 'touch_chart', 'historical_touches', 'insider_buys'}
    return {k: v for k, v in stock.items() if k not in heavy_fields}


def generate_landing_page_data(stocks: List[dict]) -> dict:
    """Generate summary data for the landing page."""
    below_line = [s for s in stocks if s['below_line']]
    approaching = [s for s in stocks if s['approaching'] and not s['below_line'] and s['pct_from_wma'] <= 15]
    # Sort approaching by distance (closest to line first)
    approaching.sort(key=lambda x: x['pct_from_wma'])

    oversold = [s for s in stocks if s['rsi_14'] < 30]
    yartseva = [s for s in stocks if s.get('yartseva_below_line')]
    buffett = [s for s in stocks if s.get('buffett_below_line')]
    aristocrats = [s for s in stocks if s.get('aristocrat_below_line')]
    cannibals = [s for s in stocks if s.get('cannibal_below_line')]
    diluting = [s for s in stocks if s.get('is_diluting')]
    insider_buying = [s for s in stocks if s.get('has_conviction_buy')]
    insider_below = [s for s in stocks if s.get('insider_below_line')]
    # Sort insider buys by largest transaction value
    insider_buying.sort(key=lambda x: x.get('largest_buy_value') or 0, reverse=True)
    insider_below.sort(key=lambda x: x.get('largest_buy_value') or 0, reverse=True)
    fcf_growing_below = [s for s in stocks if s.get('fcf_growing_below_line')]
    # Sort by FCF CAGR (strongest growers first)
    fcf_growing_below.sort(key=lambda x: x.get('fcf_cagr_3yr') or 0, reverse=True)

    return {
        # Counts
        'total_stocks': len(stocks),
        'below_line_count': len(below_line),
        'approaching_count': len(approaching),
        'oversold_count': len(oversold),
        'yartseva_count': len(yartseva),
        'buffett_count': len(buffett),
        'aristocrat_count': len(aristocrats),
        'cannibal_count': len(cannibals),
        'diluting_count': len(diluting),
        'insider_buying_count': len(insider_buying),
        'insider_below_count': len(insider_below),
        'fcf_growing_below_count': len(fcf_growing_below),
        # Stock arrays for homepage display (slimmed — no chart/history data)
        'below_line_stocks': [_slim_stock(s) for s in below_line],
        'approaching_stocks': [_slim_stock(s) for s in approaching[:20]],
        'insider_buying_stocks': [_slim_stock(s) for s in insider_buying],
        'insider_below_stocks': [_slim_stock(s) for s in insider_below],
        'fcf_growing_below_stocks': [_slim_stock(s) for s in fcf_growing_below],
    }


def load_previous_stocks(output_dir: Path) -> dict:
    """Load previous week's stocks.json for crossing detection."""
    output_file = output_dir / 'stocks.json'
    if output_file.exists():
        try:
            with open(output_file) as f:
                data = json.load(f)
            # Build lookup: symbol -> below_line status
            return {s['symbol']: s for s in data.get('stocks', [])}
        except Exception as e:
            print(f"  Warning: Could not load previous stocks.json: {e}")
    return {}


def detect_crossings(current_stocks: list, previous_stocks: dict) -> dict:
    """Detect stocks that newly crossed below or recovered above the 200WMA."""
    newly_below = []
    newly_recovered = []
    
    for stock in current_stocks:
        symbol = stock['symbol']
        prev = previous_stocks.get(symbol)
        if not prev:
            continue  # New stock, no comparison possible
        
        was_below = prev.get('below_line', False)
        is_below = stock.get('below_line', False)
        
        if is_below and not was_below:
            newly_below.append(stock)
        elif not is_below and was_below:
            newly_recovered.append(stock)
    
    # Sort newly below by depth (deepest first)
    newly_below.sort(key=lambda x: x['pct_from_wma'])
    # Sort recovered by how far above (closest to line first)
    newly_recovered.sort(key=lambda x: x['pct_from_wma'])
    
    return {
        'newly_below': newly_below,
        'newly_recovered': newly_recovered
    }


def generate_weekly_blog_post(crossings: dict, date_str: str, content_dir: Path):
    """Generate a Hugo markdown blog post for weekly crossings."""
    newly_below = crossings['newly_below']
    newly_recovered = crossings['newly_recovered']
    
    if not newly_below and not newly_recovered:
        print("\n  📝 No crossings this week — skipping blog post.")
        return None
    
    blog_dir = content_dir / 'blog'
    blog_dir.mkdir(parents=True, exist_ok=True)
    
    # Create _index.md if it doesn't exist
    index_file = blog_dir / '_index.md'
    if not index_file.exists():
        with open(index_file, 'w') as f:
            f.write("""---
title: "Weekly Signal Reports"
description: "Weekly updates on stocks crossing their 200-week moving average."
---
""")
    
    # Build the post
    slug = f"{date_str}-weekly-signal-report"
    post_file = blog_dir / f"{slug}.md"
    
    # Frontmatter
    lines = []
    lines.append('---')
    lines.append(f'title: "Weekly Signal Report — {datetime.strptime(date_str, "%Y-%m-%d").strftime("%B %d, %Y")}"')
    lines.append(f'date: {date_str}')
    lines.append(f'slug: "{slug}"')
    lines.append(f'description: "{len(newly_below)} stock{"s" if len(newly_below) != 1 else ""} crossed below and {len(newly_recovered)} recovered above the 200-week moving average this week."')
    
    # Store structured data in frontmatter for template use
    lines.append(f'newly_below_count: {len(newly_below)}')
    lines.append(f'newly_recovered_count: {len(newly_recovered)}')
    
    # Newly below symbols for frontmatter
    if newly_below:
        lines.append('newly_below:')
        for s in newly_below:
            lines.append(f'  - symbol: "{s["symbol"]}"')
            lines.append(f'    name: "{s.get("name", s["symbol"])}"')
            lines.append(f'    close: {s["close"]}')
            lines.append(f'    pct_below: {abs(s["pct_from_wma"]):.1f}')
            lines.append(f'    wma_200: {s["wma_200"]:.2f}')
            lines.append(f'    rsi_14: {s["rsi_14"]:.0f}')
            lines.append(f'    touch_count: {s.get("touch_count", 0)}')
            lines.append(f'    avg_return_after_touch: {s.get("avg_return_after_touch", 0):.1f}')
            lines.append(f'    buffett_quality: {str(bool(s.get("buffett_quality", False))).lower()}')
            lines.append(f'    dividend_aristocrat: {str(bool(s.get("dividend_aristocrat", False))).lower()}')
            lines.append(f'    yartseva_candidate: {str(bool(s.get("yartseva_candidate", False))).lower()}')
            lines.append(f'    fcf_trend: "{s.get("fcf_trend", "unknown")}"')
            lines.append(f'    fcf_yield: {s.get("fcf_yield", 0):.1f}')
            lines.append(f'    has_conviction_buy: {str(bool(s.get("has_conviction_buy", False))).lower()}')
            lines.append(f'    sector: "{s.get("sector", "")}"')
            # Market cap as readable string
            mc = s.get('market_cap', 0) or 0
            if mc >= 1e12:
                lines.append(f'    market_cap_display: "${mc/1e12:.1f}T"')
            elif mc >= 1e9:
                lines.append(f'    market_cap_display: "${mc/1e9:.1f}B"')
            elif mc >= 1e6:
                lines.append(f'    market_cap_display: "${mc/1e6:.0f}M"')
            else:
                lines.append(f'    market_cap_display: "N/A"')
    
    if newly_recovered:
        lines.append('newly_recovered:')
        for s in newly_recovered:
            lines.append(f'  - symbol: "{s["symbol"]}"')
            lines.append(f'    name: "{s.get("name", s["symbol"])}"')
            lines.append(f'    close: {s["close"]}')
            lines.append(f'    pct_above: {s["pct_from_wma"]:.1f}')
            lines.append(f'    wma_200: {s["wma_200"]:.2f}')
    
    lines.append('---')
    lines.append('')
    
    # Body content — a brief intro (the template will render the structured data)
    if newly_below:
        lines.append(f'This week, {len(newly_below)} stock{"s" if len(newly_below) != 1 else ""} crossed below {"their" if len(newly_below) != 1 else "its"} 200-week moving average — entering what we call "deep value territory." This is the signal our screener is built to detect: quality companies trading below a price level that has historically represented a floor over the prior four years.')
        lines.append('')
        lines.append('Not every stock that crosses the line is a buy. Some are cheap for good reason. The 200-week moving average is a starting point for research, not a buy signal. Below, we break down each new crossing with the context you need to decide whether it\'s opportunity or a warning.')
        lines.append('')
    
    if newly_recovered:
        if newly_below:
            lines.append(f'On the other side, {len(newly_recovered)} stock{"s" if len(newly_recovered) != 1 else ""} climbed back above the line this week — exiting deep value territory.')
        else:
            lines.append(f'No stocks crossed below the line this week, but {len(newly_recovered)} stock{"s" if len(newly_recovered) != 1 else ""} climbed back above — exiting deep value territory.')
        lines.append('')
    
    with open(post_file, 'w') as f:
        f.write('\n'.join(lines))
    
    print(f"\n  📝 Blog post generated: {post_file.name}")
    print(f"     {len(newly_below)} newly below, {len(newly_recovered)} recovered")
    return post_file


def main():
    """Main pipeline."""
    print("=" * 60)
    print("Below The Line - Stock Data Pipeline")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    company_metadata = load_company_metadata()
    print(f"Loaded {len(company_metadata)} company records")
    
    # Load previous week's data BEFORE overwriting
    previous_stocks = load_previous_stocks(OUTPUT_DIR)
    print(f"Loaded {len(previous_stocks)} previous stock records for crossing detection")
    
    # Fetch SPY benchmark data once
    spy_monthly = fetch_spy_monthly()
    
    all_stocks = []
    errors = []
    
    total = len(STOCK_UNIVERSE)
    for i, symbol in enumerate(STOCK_UNIVERSE):
        print(f"\n[{i+1}/{total}]", end="")
        
        try:
            result = calculate_stock_signals(symbol, spy_monthly=spy_monthly)
            if result:
                # Merge company metadata (name, sector, ir_url)
                meta = company_metadata.get(symbol, {})
                result['name'] = meta.get('name', '')
                result['sector'] = meta.get('sector', '')
                result['ir_url'] = meta.get('ir_url', '')
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
        'stocks': all_stocks,
        'generated_readable': datetime.now().strftime('%B %d, %Y'),
        'generated_iso': datetime.now().strftime('%Y-%m-%d')
    }
    
    output_file = OUTPUT_DIR / 'stocks.json'
    with open(output_file, 'w') as f:
        json.dump(output, f, separators=(',', ':'), cls=NumpyEncoder)
    
    # Detect crossings and generate blog post
    skip_blog = os.environ.get('SKIP_BLOG', 'false').lower() == 'true'
    if skip_blog:
        print("\n  📝 SKIP_BLOG set — skipping blog generation (manual run).")
    elif previous_stocks:
        crossings = detect_crossings(all_stocks, previous_stocks)
        content_dir = Path(__file__).parent.parent / 'content'
        date_str = datetime.now().strftime('%Y-%m-%d')
        generate_weekly_blog_post(crossings, date_str, content_dir)
    else:
        print("\n  📝 No previous data — skipping blog generation (first run).")
    
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
