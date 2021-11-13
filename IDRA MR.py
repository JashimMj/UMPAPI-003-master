import requests,json
# import mysql.connector
import time
import array
import cx_Oracle

delay=30*1    ###for 30 second
close_time=time.time()+delay
cnx = cx_Oracle.connect('jashim/jashim@//localhost:1521/orcl')
print(cnx.version)
mycursor = cnx.cursor()
mycursor.execute("""SELECT
'' AS "mrSerialNumber"
,  a.branchcode AS "officeBranchCode"
,  a.branchname AS "officeBranchName"
,  a.mrNumber AS "mrNumber"
,  to_char(a.mrDate, 'YYYY-MM-DD') AS "mrDate"
,  a.classInsurance AS "classInsurance"
,  a.insuredName AS "insuredName"
,  a.insuredAddress AS "insuredAddress"
,  a.insuredMobile AS "insuredMobile"
,  a.insuredEmail AS "insuredEmail"
,  a.modeOfPayment AS "modeOfPayment"
,  a.paymentDetail AS "paymentDetail"
,  a.coverNoteNumber AS "coverNoteNumber"
,  a.policyNumber AS "policyNumber"
,  a.addendumNumber AS "addendumNumber"
,  a.endorsementNumber AS "endorsementNumber"
,  a.netPremium AS "netPremium"
,  a.vat AS "vat"
,  a.stamp AS "stamp"
,  a."others" AS "others"
,  a.totalPremium AS "totalPremium"
,  a.chequeDrawnOn AS "chequeDrawnOn"
,  to_char(a.chequeDate, 'YYYY-MM-DD') AS "chequeDate"
,  to_char(a.depositDate, 'YYYY-MM-DD') AS "depositDate"
,  a.depositedToBank AS "depositedToBank"
, '' AS "depositedToBranch"
, '' AS "depostiedToAccountNumber"
, '' AS "mfs"
, '' AS "mfsAccountNumber"
,  a.isCoInsurance AS "isCoInsurance"
,  a.isLeader AS "isLeader"
,  a.financingBankName AS "financingBankName"
,  a.financingBankAddress AS "financingBankAddress"
, '' AS "financingBankEmail"
, '' AS "financingBankMobile"
,  a.isMultiDocument AS "isMultiDocument"
,  a.bankGuaranteeNumber AS "bankGuaranteeNumber",
a.ump as "UmP",
a.mrno as "MrNo"
FROM (
-- 1
SELECT
m.BRANCHCODE AS branchcode, b.BNAME AS branchname, b.SHORTNAME || '-' || to_char(m."YEAR") || '-' || m.MRNO AS mrNumber,m.ump as ump,m.mrno as mrno,
m.MRDATE, m.C_CLASS AS classInsurance, m.cl_name AS insuredName, m.cl_add AS insuredAddress,
nvl(c.MOBILE, c.phone) AS insuredMobile, c.email AS insuredEmail, d.MOP AS modeOfPayment, m.CK_NO AS paymentDetail,
nvl2(m.ADDN_NO, m.ADDN_COVER_NOTE_NO, m.CERT_NO) AS covernoteNumber,
nvl2(m.END_NO, m.END_POLICY_NO, m.policy_no) AS policyNumber,
m.ADDN_NO AS addendumNumber, m.END_NO AS endorsementNumber,
m.MR_BANK AS chequeDrawnOn, m.CK_DT AS chequeDate, d.DEPOSIT_DATE AS depositDate, db.BANK_NAME AS depositedToBank,
decode(m.COINS, '1', 'Y', 'N') AS isCoInsurance,
(case when m.COINS = '1' and m.LEADER = '1'  then 'Y' else '' END) isLeader, m.BG_NO AS bankGuaranteeNumber,
m.BANK_NAME AS financingBankName, m.BANK_BRANCH AS financingBankBranch, m.BANK_ADD AS financingBankAddress, '' AS isMultiDocument
,m.premium AS netPremium, nvl(m.vat, 0) AS vat, nvl(m.stamp, 0) AS stamp, nvl(m.SERVICE_CHARGE, 0) AS "others", m.total AS totalPremium
FROM mast m LEFT JOIN mr_deposit d ON d.uw_mst_id = m.mst_id
LEFT JOIN uw_client c ON m.CLIENTCODE = c.idno
LEFT JOIN DEPOSIT_BANK db ON db.BANK_CODE = d.BANK_CODE
LEFT JOIN Branch b ON b.BCODE = m.BRANCHCODE
where 1 = 1
AND m.total > 0
AND (m.INSTYPE IS NULL OR (m.C_CLASS = 'Marine Cargo' AND m.INSTYPE NOT IN ('Open' , 'Certificat')))
UNION ALL
-- 2
SELECT
m.BRANCHCODE AS branchcode, b.BNAME AS branchname
,b.SHORTNAME || '-' || to_char(m."YEAR") || '-' || m.MRNO AS mrNumber,m.ump as ump,m.mrno as mrno
,m.MRDATE, m.C_CLASS AS classInsurance, m.cl_name AS insuredName, m.cl_add AS insuredAddress,
nvl(c.MOBILE, c.phone) AS insuredMobile, c.email AS insuredEmail, d.MOP AS modeOfPayment, m.CK_NO AS paymentDetail,
nvl2(m.ADDN_NO, m.ADDN_COVER_NOTE_NO, m.CERT_NO) AS covernoteNumber,
nvl2(m.END_NO, m.END_POLICY_NO, m.policy_no) AS policyNumber,
m.ADDN_NO AS addendumNumber, m.END_NO AS endorsementNumber,
m.MR_BANK AS chequeDrawnOn, m.CK_DT AS chequeDate, d.DEPOSIT_DATE AS depositDate, db.BANK_NAME AS depositedToBank,
decode(m.COINS, '1', 'Y', 'N') AS isCoInsurance,
(case when m.COINS = '1' and m.LEADER = '1'  then 'Y' else '' END) isLeader, m.BG_NO AS bankGuaranteeNumber,
'' AS financingBankName, '' AS financingBankBranch, '' AS financingBankAddress, 'Y' AS isMultiDocument
,sum(m.premium)  AS netPremium, sum(nvl(m.vat, 0)) AS vat, sum(nvl(m.stamp, 0)) AS stamp, sum(nvl(m.SERVICE_CHARGE, 0)) AS "others", sum(m.total) AS totalPremium
FROM mr_deposit d LEFT JOIN mast m ON d.MR_NO = m.mrno
LEFT JOIN uw_client c ON m.CLIENTCODE = c.idno
LEFT JOIN DEPOSIT_BANK db ON db.BANK_CODE = d.BANK_CODE
LEFT JOIN Branch b ON b.BCODE = m.BRANCHCODE
where 1 = 1
AND d.BRANCHCODE = m.BRANCHCODE
AND d."YEAR" = m."YEAR"
AND m.total > 0
AND m.C_CLASS = 'Marine Cargo' AND m.INSTYPE = 'Open'
GROUP BY
m.BRANCHCODE, b.BNAME,m.ump,m.mrno,
b.SHORTNAME || '-' || to_char(m."YEAR") || '-' || m.MRNO
,m.MRDATE, m.C_CLASS, m.cl_name, m.cl_add,
nvl(c.MOBILE, c.phone), c.email, d.MOP, m.CK_NO,
nvl2(m.ADDN_NO, m.ADDN_COVER_NOTE_NO, m.CERT_NO),
nvl2(m.END_NO, m.END_POLICY_NO, m.policy_no),
m.ADDN_NO, m.END_NO, m.MR_BANK, m.CK_DT, d.DEPOSIT_DATE, db.BANK_NAME,
decode(m.COINS, '1', 'Y', 'N'),
(case when m.COINS = '1' and m.LEADER = '1'  then 'Y' else '' END), m.BG_NO
UNION ALL
-- 3
SELECT
m.BRANCHCODE AS branchcode, b.BNAME AS branchname
,b.SHORTNAME || '-' || to_char(m."YEAR") || '-' || m.MRNO AS mrNumber,m.ump as ump,m.mrno as mrno
,m.MRDATE, m.C_CLASS AS classInsurance, m.cl_name AS insuredName, m.cl_add AS insuredAddress,
nvl(c.MOBILE, c.phone) AS insuredMobile, c.email AS insuredEmail, d.MOP AS modeOfPayment, m.CK_NO AS paymentDetail,
'' AS covernoteNumber,
nvl2(m.END_NO, m.END_POLICY_NO, m.policy_no) AS policyNumber,
m.ADDN_NO AS addendumNumber, m.END_NO AS endorsementNumber,
m.MR_BANK AS chequeDrawnOn, m.CK_DT AS chequeDate, d.DEPOSIT_DATE AS depositDate, db.BANK_NAME AS depositedToBank,
decode(m.COINS, '1', 'Y', 'N') AS isCoInsurance,
(case when m.COINS = '1' and m.LEADER = '1'  then 'Y' else '' END) isLeader, m.BG_NO AS bankGuaranteeNumber,
'' AS financingBankName, '' AS financingBankBranch, '' AS financingBankAddress, 'Y' AS isMultiDocument
,sum(m.premium)  AS netPremium, sum(nvl(m.vat, 0)) AS vat, sum(nvl(m.stamp, 0)) AS stamp, sum(nvl(m.SERVICE_CHARGE, 0)) AS "others", sum(m.total) AS totalPremium
FROM mr_deposit d LEFT JOIN mast m ON d.MR_NO = m.mrno
LEFT JOIN uw_client c ON m.CLIENTCODE = c.idno
LEFT JOIN DEPOSIT_BANK db ON db.BANK_CODE = d.BANK_CODE
LEFT JOIN Branch b ON b.BCODE = m.BRANCHCODE
where 1 = 1
AND d.BRANCHCODE = m.BRANCHCODE
AND d."YEAR" = m."YEAR"
AND m.total > 0
AND m.C_CLASS = 'Marine Cargo' AND m.INSTYPE = 'Certificat'
GROUP BY
m.INSTYPE, m.CLASS_SUB_TYPE,m.ump,m.mrno,
m.BRANCHCODE, b.BNAME,
b.SHORTNAME || '-' || to_char(m."YEAR") || '-' || m.MRNO
,m.MRDATE, m.C_CLASS, m.cl_name, m.cl_add,
nvl(c.MOBILE, c.phone), c.email, d.MOP, m.CK_NO,
nvl2(m.END_NO, m.END_POLICY_NO, m.policy_no),
m.ADDN_NO, m.END_NO, m.MR_BANK, m.CK_DT, d.DEPOSIT_DATE, db.BANK_NAME,
decode(m.COINS, '1', 'Y', 'N'),
(case when m.COINS = '1' and m.LEADER = '1'  then 'Y' else '' END), m.BG_NO
) a
WHERE 1 = 1
and a.MRDATE >= to_date('01-10-2020', 'DD-MM-YYYY') 
-- AND a.BRANCHCODE IN (5)
AND a.BRANCHCODE IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,14,15)
and a.ump is null
order by mrNumber
""")
myresult = mycursor.fetchall()
while True:
    for (mrSerialNumber,officeBranchCode,officeBranchName,mrNumber,
    mrDate,
    classInsurance,
    insuredName,
    insuredAddress,
    insuredMobile,
    insuredEmail,
    modeOfPayment,
    paymentDetail,
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
    bankGuaranteeNumber,UmP,MrNo) in myresult:
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
                    "vat": vat,
                    "stamp": stamp,
                    "others": others,
                    "totalPremium": totalPremium,
                    "chequeDrawnOn": chequeDrawnOn,
                    "chequeDate": chequeDate,
                    "depositDate": depositDate,
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
                    "isMultiDocument": isMultiDocument,
                    "bankGuaranteeNumber": bankGuaranteeNumber}
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

        mycursor.execute("update mast set ump=1 where mrno =:mrno",[MrNo])
        cnx.commit()
        print(payloads)
