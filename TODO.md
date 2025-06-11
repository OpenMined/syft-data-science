# Long term
- We can switch to full HTTP-based RDS client
  - difficulties:
    - Datasets are right now filesync based
    - Job outputs
    - Usercode (once uploaded)
  - This would solve permissions, because we can do permissions on the RDS server (instead of file permissions)
  - Downside: Its less file-based, more API-based
  - Downside: its also less offline-first, server needs to be online
  - Maybe we can do a hybrid approach, where we have a HTTP-based client with filesync fallback

# Short term
- Fix user_file_service permissions, right now we cannot share job results to DS
  - currently, either everyone can see, or only the DO (both are bad and need to be fixed)
- app.yaml permissions (or remove app.yaml?)
- Talk to Rasswanth about permissions and format
