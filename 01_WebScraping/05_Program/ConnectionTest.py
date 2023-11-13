#%% Import 
import warnings
warnings.filterwarnings("ignore") # Ignore all warnings
from zDMSMasterData import DMSMasterData
from zRBMasterData import RBMasterData
from zDMSAggPriceResult import DMSAggPriceResult
from zO2CPriceData import O2CPriceData

while True:
    #------------------------------------------------------------------------------
    print("===== Loading Data from DMS Database =====")
    ## One2Car MASTER -----------------------------------------
    print("Loading One2Car Data")
    df_O2Cprocessor = O2CPriceData()
    df_O2C = df_O2Cprocessor.get_data_from_db()  # Call the method with parentheses
    print("One2Car Master Data:", df_O2C.shape , "\n", df_O2C.head(3))
    # Define the columns to check for duplicates
    columns = ['Brand', 'Model', 'SubModel', 'Year', 'Gear', 'Fuel', 'Color_flg', 'CarTypes', 'Count',
                'Mean_Price', 'Median_Price', 'Min_Price', 'Max_Price', 'Diff_Price']     
    print("Number of duplicate rows before removal:", df_O2Cprocessor.count_duplicates(subset=columns))
    df_O2C = df_O2Cprocessor.drop_duplicates(subset=columns, inplace=True)
    print("Number of duplicate rows after removal:", df_O2Cprocessor.count_duplicates(subset=columns))
    print()

    ## DMS MASTER -----------------------------------------
    print("Loading DMS MASTER Data")
    df_DMSprocessor = DMSMasterData()
    df_DMS = df_DMSprocessor.get_data_from_db()  # Call the method with parentheses
    print("DMS Master Data:", df_DMS.shape , "\n", df_DMS.head(3))
    # Define the columns to check for duplicates
    columns = ['BrandCode', 'BrandNameEng', 'ModelCode', 'ModelName', 'SubModelCode', 'SubModelName']     
    print("Number of duplicate rows before removal:", df_DMSprocessor.count_duplicates(subset=columns))
    df_DMS = df_DMSprocessor.drop_duplicates(subset=columns, inplace=True)
    print("Number of duplicate rows after removal:", df_DMSprocessor.count_duplicates(subset=columns))
    print()

    ## RedBook MASTER -----------------------------------------
    print("Loading RedBook MASTER Data")
    df_RBprocessor = RBMasterData()
    df_RB = df_RBprocessor.get_data_from_db()
    # Define the columns to check for duplicates
    columns = ['RBBandCode', 'RBBrand', 'RBModel', 'RBSubModel']
    print("RedBook Master Data:", df_RB.shape , "\n", df_RB.head(3))
    print("Number of duplicate rows before removal:", df_RBprocessor.count_duplicates(subset=columns))
    df_RB = df_RBprocessor.drop_duplicates(subset=columns, inplace=True)
    print("Number of duplicate rows after removal:", df_RBprocessor.count_duplicates(subset=columns))
    print()

    ## DMS Price -----------------------------------------
    print("Loading Price of DMS Database")
    df_DMSPriceprocessor = DMSAggPriceResult()
    df_DMSPrice = df_DMSPriceprocessor.run()
    print("Price of DMS Database:", df_DMSPrice.shape , "\n", df_DMSPrice.head(3))

    print()
    print("===== All Data Loading Complete =====")

    # User decision to exit or continue
    user_decision = input("Process complete. Press Enter to exit or type 'continue' to keep running: ")

    if user_decision.lower() != 'continue':
        print("Exiting script.")
        # You can add any cleanup or closing operations here if necessary
    else:
        print("Continuing with further operations...")
