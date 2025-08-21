## Loss Severity Modeling of Single-Family Residential Mortgage Loans

This project incorporates a deep investigation of mortgage loan data extracted from Fannie Mae. The goal of this project is to predict the loss given default using different predictors, including macroeconomic parameters. The project can be divided into two main segments sequentially. The first is feature engineering through exploratory data analysis and data cleaning. The second part includes applying Machine Learning models, evaluating them, and comparing results to improve accuracy.

Mortgage loan data analysis is very relevant because mortgage loans are among the most widely sold credit products and deeply influence the financial industry. Analyzing mortgage loans also provides important insights into credit risk modeling, as many credit derivatives issued by lenders are structurally similar to mortgage loans.

### Data Extraction - Feature Engineering 

🟦 Acquisition Data (static loan characteristics)

These variables describe the loan at origination and do not change over time. They include:

Loan details – Original unpaid principal balance (UPB), original loan term (months), origination date, first scheduled payment date, original loan-to-value (LTV) and combined LTV ratios, purpose of loan (purchase, refinance, etc.)

Credit details of borrower – Number of borrowers, debt-to-income ratio at origination, borrower(s) credit scores, first-time homebuyer flag

Lender details – Origination channel (e.g., broker, retail, etc.), original interest rate (%)

Property details – Property type (single-family, condo, etc.), number of units, occupancy status (owner-occupied, investor, etc.)

Geographic identifiers – State, ZIP (3-digit), MSA

Mortgage insurance – Coverage percentage and type (mi_pct, mi_type)

Product type – Fixed, ARM, etc. (prod_type)

Indicator flags – HomeReady program, relocation loans, property inspection waiver, high balance loans, HLTV refinance option (hrp_ind, relo_flg, prop_ins_ind, hbl_ind, hltv_ref_ind)

🟧 Performance Data (dynamic loan status over time)

These variables track monthly loan performance until termination. They include:

Current reporting period (YYYYMM)

Current servicer of the loan

Current interest rate (curr_rte), current unpaid principal balance

Loan age (months since origination), remaining months to maturity (scheduled and adjusted)

Delinquency status (0 = current, 1+ = months delinquent)

Modification indicator (Y/N)

Zero balance code and effective date (reason loan ended)

Last paid installment date

Foreclosure and disposition dates (if applicable)

Foreclosure and property holding expenses, net sales proceeds, and other recovery proceeds

Non-interest bearing UPB and principal forgiveness amounts

Last UPB at zero balance

#### Derived Features (aggregated from monthly data)

We derive features from the performance data and append them to the acquisition data. These features represent flattened components of the time series loan performance, which would otherwise require explicit time series modeling.

![Feature Deriving Pipeline](plantUML.jpeg "Feature Pipeline")

#### External variables added 
Additional predictive variables were included, such as the Housing Price Index (HPI). We also incorporated mark-to-market LTV measures.

### Prediction models - Improving Accuracy
The first model implemented was linear regression, which served as a benchmark for the predictive power of the derived features and external variables. Once we finalized the feature set, we proceeded to improve prediction accuracy using machine learning models. For evaluation, the data was divided into training and testing sets.

We applied subset selection methods to improve model efficiency by choosing a smaller set of predictors that provided similar accuracy. Subsequently, higher-order statistical models such as Support Vector Machines (SVMs) and Spline Regression were used.

### User Guide

Downlod data from - [Data-dynamics](https://www.fanniemae.com/data-dynamics), Login to- pingone and go to - 'Historical Loan Credit Performance Data'. On the left side menu click on Download data. Clikc on all the desired Quaterly datasets to start download.

The Driver_code.R sources the feature Engineerin- Data Extraction code. Alter these variables to rund the extraction for the years of data that you downloaded.
starting_file <- x # x= (start_ year - 2000) x 4 + Quater -1 , Eg - 2000Q1 is 0
ending_file <- y #  y= (end_ year - 2000) x 4 + Quater -1 , Eg - 2023Q4 is 95 