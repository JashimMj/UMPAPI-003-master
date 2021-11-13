import requests,json
# import mysql.connector
from time import time, sleep
import array
import cx_Oracle

# delay=60*1    ###for 60 second
# close_time=time.time()+delay
cnx = cx_Oracle.connect('jashim/jashim@//localhost:1521/orcl')
print(cnx.version)
while True:
    sleep(60 - time() % 60)
    mycursor = cnx.cursor()
    mycursor.execute("BEGIN LOAD_MONEY_RECEIPT(); END;")
    mycursor.execute("""SELECT
    mrSerialNumber,                      
    officeBranchCode,                   
    officeBranchName,                   
    mrNumber,                          
    mrDate,                              
    classInsurance,                      
    insuredName,                         
    insuredAddress,                     
    insuredMobile,                       
    insuredEmail,                        
    modeOfPayment,                      
    paymentDetail ,                     
    coverNoteNumber,                  
    policyNumber,                
    addendumNumber,            
    endorsementNumber,                
    netPremium,                          
    vat,                                 
    stamp,                          
    others,                            
    totalPremium,                      
    chequeDrawnOn,                   
    chequeDate,                          
    depositDate,                      
    depositedToBank,                    
    depositedToBranch,                  
    depositedToAccountNumber,          
    mfs,                               
    mfsAccountNumber,                   
    isCoInsurance,                     
    isLeader,                            
    financingBankName,                  
    financingBankAddress,               
    financingBankEmail,                  
    financingBankMobile,                
    isMultiDocument,                    
    multiDocuments,                     
    currency,             
    leaderDocument,                     
    paymentReceivedFrom,                
    serviceCharge,                  
    coInsurerPremiumAmount,             
    bankGuaranteeNumber,                 
    requeston ,                         
    responseon,                          
    response,                            
    mrURL,                               
    umpStatus,                          
    depositStatus from ump_mr where umpStatus ='N'                
    """)
    myresults = mycursor.fetchall()
    for(mrSerialNumber,
    officeBranchCode,
    officeBranchName,
    mrNumber,
    mrDate,
    classInsurance,
    insuredName,
    insuredAddress,
    insuredMobile,
    insuredEmail,
    modeOfPayment,
    paymentDetail ,
    coverNoteNumber,
    policyNumber,
    addendumNumber,
    endorsementNumber,
    netPremium,
    vat,
    stamp,
    others,
    totalPremium,
    chequeDrawnOn,
    chequeDate,
    depositDate,
    depositedToBank,
    depositedToBranch,
    depositedToAccountNumber,
    mfs,
    mfsAccountNumber,
    isCoInsurance,
    isLeader,
    financingBankName,
    financingBankAddress,
    financingBankEmail,
    financingBankMobile,
    isMultiDocument,
    multiDocuments,
    currency,
    leaderDocument,
    paymentReceivedFrom,
    serviceCharge,
    coInsurerPremiumAmount,
    bankGuaranteeNumber,
    requeston ,
    responseon,
    response,
    mrURL,
    umpStatus,
    depositStatus) in myresults:

        payload = {'client_id': 'paramount', 'client_secret': 'admin'}
        r = requests.post('https://idra-ump.com/test/app/extern/v1/authenticate', json=payload)
        access_para = json.loads(r.text)
        access_tokenpara = access_para['access_token']
        refresh_para = json.loads(r.text)
        refresh_tokenpara = refresh_para['refresh_token']
        token_para = json.loads(r.text)
        token_typepara = token_para['token_type']
        payloads = {"mrSerialNumber": mrSerialNumber,
                    "officeBranchCode": str(officeBranchCode),
                    "officeBranchName": officeBranchName,
                    "mrNumber": mrNumber,
                    "mrDate": mrDate,
                    "classInsurance": classInsurance,
                    "insuredName": insuredName,
                    "insuredAddress": insuredAddress,
                    "insuredMobile": insuredMobile,
                    "insuredEmail": insuredEmail,
                    "modeOfPayment": modeOfPayment,
                    "paymentDetail": paymentDetail,
                    "coverNoteNumber": coverNoteNumber,
                    "policyNumber": policyNumber,
                    "addendumNumber": addendumNumber,
                    "endorsementNumber": endorsementNumber,
                    "netPremium": netPremium,
                    "vat":vat,
                    "stamp": stamp,
                    "others": others,
                    "totalPremium": totalPremium,
                    "chequeDrawnOn": chequeDrawnOn,
                    "chequeDate": chequeDate,
                    "depositDate":depositDate,
                    "depositedToBank": depositedToBank,
                    "depositedToBranch": depositedToBranch,
                    "depositedToAccountNumber": depositedToAccountNumber,
                    "mfs": mfs,
                    "mfsAccountNumber": mfsAccountNumber,
                    "isCoInsurance": isCoInsurance,
                    "isLeader": isLeader,
                    "financingBankName": financingBankName,
                    "financingBankAddress": financingBankAddress,
                    "financingBankEmail": financingBankEmail,
                    "financingBankMobile": financingBankMobile,
                    "bankGuaranteeNumber": bankGuaranteeNumber,
                    "isMultiDocument": isMultiDocument,
                    "currency":currency,
                    "serviceCharge":serviceCharge,
                    "leaderDocument":leaderDocument,
                    "paymentReceivedFrom":paymentReceivedFrom,
                    "coInsurerPremiumAmount":coInsurerPremiumAmount,
                    "multiDocuments":multiDocuments}

        print(payloads)

        # print(payloads)
        # try:
        #     ab = requests.post('https://idra-ump.com/test/app/extern/v1/money-receipt', json=payloads,headers={'Authorization': f"Bearer {access_tokenpara}"})
        #     print(ab.json())
        # except:
        #     print("data not error")
        # mycursor.execute("select * from mast where mrno = :mrno and ump is null",[MrNo])
        # a = mycursor.fetchall()
        # for x in a:
        #     print(x)

        # mycursor.execute("update ump_mr set umpStatus='Y' where mrNumber =:mrNumber",[mrNumber])
        # cnx.commit()






