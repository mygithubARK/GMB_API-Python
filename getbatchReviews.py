from googleapiclient import sample_tools
from googleapiclient.http import build_http
import sys
import json
import pandas as pd

discovery_doc = "gmb_discovery.json"

def main (argv):

    # use the discovery doc to build a service that we can use to 
    # make MyBusiness API calls and authenticate the user so we can access their account

    service, flags = sample_tools.init(argv, "mybusiness", "v4", __doc__, __file__, scope="https://www.googleapis.com/auth/business.manage", discovery_filename=discovery_doc)

    ## STEP 1 - Get the List of all accounts for the authenticated user
    # GMB API call -- Method:accounts.list

    output = service.accounts().list().execute()
    print("List of GMB Accounts: \n\n")
    print(json.dumps(output, indent=2) + "\n")
    # extract the account name which will be used for further API calls
    gmbAccount = output["accounts"][1]["name"]

    ## STEP 2 - Get the list of all available locations for the specified account (gmbAccount)
    # Limitation - 100 locations fetched per API Call
    # we use 'pageToken' - to fetch all the available locations

    try:
        page_token = None
        # Defining empty dataframe and columns names where extracted info will be stored
        loc_df = pd.DataFrame()
        column_names = ['locationId', 'StoreID', 'Street', 'PostalCode', 'City', 'CountryCode', 'PrimaryPhone']
        while True:
            print("Fetching list of locations for account " + gmbAccount, "\n\n")
            # GMB API Call - Method:accounts.locations.list
            loclist = service.accounts().locations().list(parent=gmbAccount,pageToken=page_token).execute()
            print(json.dumps(loclist, indent=2))

            # Extracting only the necessary information from the response and append it to dataframe
            for i in loclist['locations']:
                name = i['name']
                storeCode = i['storeCode']
                address = i['address']['addressLines'][0]
                postalCode = i['address']['postalCode']
                locality = i['address']['locality']
                regionCode = i['address']['locality']
                primaryPhone = i['primaryPhone']
                loc_df = loc_df.append(pd.Series([name, storeCode, address, postalCode, locality, regionCode, primaryPhone]),ignore_index=True)
                            
            # Checking for the 'nextPageToken' in the response
            # if not available then break the loop
            page_token = loclist.get('nextPageToken')
            if not page_token:
                break
    
    finally:
        print("All locations fetched for the account")
        print("Next Page Token"+str(page_token))
        loc_df.columns = column_names
        #loc_df.to_csv('temp/allinone.csv', index=False)
    
    ## STEP 3: Getting the review data for each locationId

    # Defining an empty dataframe and column names for storing the extracted review data
    review_df = pd.DataFrame()
    columns1 = ['locationId', 'ReviewerName', 'StarRating', 'ReviewCreateTime', 'ReviewerComments']
    
    # Loop over each locationId and generate request body for the API call
    for x in loc_df['locationId']:
        body = {
            "locationNames": [
                x
            ]
        }
        print("Getting reviews for locationId " +x)
        
        # GMB API Call - Method:accounts.locations.batchGetReviews 
        revlist = service.accounts().locations().batchGetReviews(name=gmbAccount,body=body).execute()
        
        # extracting necessary information from the response message
        for j in revlist['locationReviews']:
            locationId = j['name']
            ReviewerName = j['review']['reviewer']['displayName']
            ReviewerRating = j['review']['starRating']
            ReviewUpdateTime = j['review']['createTime']
            ReviewerComments = j['review'].get('comment','NONE')

            # appending the extracted values into dataframe
            review_df = review_df.append(pd.Series([locationId, ReviewerName, ReviewerRating, ReviewUpdateTime, ReviewerComments]), ignore_index=True)

    # Appending all location data and review data into single dataframe
    review_df.columns = columns1
    combined = pd.merge(loc_df, review_df, on='locationId')
    combined.to_csv('temp/allmerged.csv', index=False)
    #print(pd.merge(loc_df, review_df, on='locationId'))

    
    # Writing pandas Dataframe to Google BigQuery
    
    # Option1: Upload Dataframe using 'pandas.DataFrame.to_gbq()' function
    ## Option2: Saving Dataframe as csv and then upload as a file to BigQuery using the Python API
    ### Option3: Saving Dataframe as csv and then upload the file to Google Cloud Storage and then reading it from BigQuery

if __name__ == "__main__":
    main(sys.argv)
