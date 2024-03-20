# Archive Universal Analytics (UA) Data Using Google Analytics API and Google Colab 
How to run this notebook in Google Colab.

* Create a GCP project. In APIs and Services, enable 'Analytics Reporting API' and 'Cloud Storage' (if you want to export to cloud storage).
* Create a service account for this service, and download the key JSON.
* If you are saving your data to Cloud Storage, create your bucket and give your service account write rights to your bucket (... on the right -> Add principal).
* If you are saving your data to Google Drive, create your folder.
* Open the notebook in Colab and save it to Google Drive.
* Click on the key in the left pane of the notebook in Google Colab and save the key JSON text as an environmental variable called `ua_sa_key`.
* Follow the instructions in the notebook.
