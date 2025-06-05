import os
import requests
import pandas as pd
import configparser
import json
import jwt
import datetime
import re

config = configparser.ConfigParser()
config.read("/home/user/.ssh/config.ini")


class Aem:
    def __init__(self, client_id, client_secret, sf_url, private_key_path):
        self.client_id = client_id
        self.client_secret = client_secret
        self.private_key_path = private_key_path
        self.access_token = self.get_token()
        self.sf_url = sf_url

    def get_token(self):
        url = 'https://ims-na1.adobelogin.com/ims/exchange/jwt'
        jwtPayloadRaw = """placeholder"""
        jwtPayloadJson = json.loads(jwtPayloadRaw)
        jwtPayloadJson["exp"] = datetime.datetime.utcnow() + \
            datetime.timedelta(seconds=30)

        accessTokenRequestPayload = {
            'client_id': self.client_id, 'client_secret': self.client_secret}

        keyfile = open(self.private_key_path, 'r')
        private_key = keyfile.read()
        # print(private_key)

        jwttoken = jwt.encode(jwtPayloadJson, private_key, algorithm='RS256')

        accessTokenRequestPayload['jwt_token'] = jwttoken
        result = requests.post(url, data=accessTokenRequestPayload, timeout=5)
        resultjson = json.loads(result.text)

        # print(resultjson["access_token"])
        return resultjson["access_token"]

    def get_doc_list(self, path, date_from, date_to):
        try:
            headers = {"Authorization": f"Bearer {self.access_token}"}
            url = f"{self.sf_url}/bin/querybuilder.json?path={path}/&type=dam:Asset&p.limit=-1&p.hits=full&p.nodedepth=2&1_property=jcr:content/cq:lastReplicationAction&2_daterange.property=jcr:content/cq:lastReplicated&2_daterange.lowerBound={date_from}&2_daterange.upperBound={date_to}"

            response = requests.get(url, headers=headers)
            data = response.json()
            return data.get("hits")
        except Exception as e:
            print(f"Error while fetching AEM document list: {e}")
            return []

    def download_asset(self, url, dest_path):

        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = requests.get(url, headers=headers)

        # Check if the request was successful.
        if response.status_code == 200:
            # Save the file to disk.
            with open(dest_path, "wb") as f:
                f.write(response.content)
        else:
            # Handle the error.
            print("Error downloading the file.")


if __name__ == "__main__":
    os.environ["REQUESTS_CA_BUNDLE"] = "/etc/ssl/certs/ca-bundle.pem"

    env = "DEVELOPMENT"
    # env = "TEST"
    # env = "PRODUCTION"
    try:
        aem = Aem(
            "cm-p143511-e1511115-integration-0",
            "p8e-L61UPA6CIKodORt0BBKn6526Yis8UU5Q",
            "https://author-p143511-e1511115.adobeaemcloud.com/",
            "/home/user/.ssh/aem/prod_private.key"
        )

        # aem.download_asset(config[env]["aem_cloud.url"] + "/content/dam/micron/global/secure/products/data-sheet/modules/sodimm/dd4c16-32x64h.pdf",
        #                    "/home/user/develop/genai/sales/aem_cloud/dd4c16-32x64h.pdf")
        aem.sf_url = "https://author-p143511-e1511115.adobeaemcloud.com"
        path = "https://author-p143511-e1511115.adobeaemcloud.com/"
        doc_list = aem.get_doc_list("/content/dam/micron/global/secure/products/sim-model", "2020-02-01T00:00:00", "2025-05-15T00:00:00")
        df = pd.json_normalize(doc_list)
        # df.to_csv(
        #     "/home/jupyter/dev/genai/sales/temp/aem/aem_metadata.csv",
        #     header=True,
        #     index=False,
        # )
        for index, row in df.iterrows():
            file_name_with_ext = os.path.basename(row["jcr:path"])
            file_name, file_ext = os.path.splitext(file_name_with_ext)
            file_name = file_name.replace(" ", "_")
            # des_path = f"/home/jupyter/dev/genai/sales/temp/aem/{file_name}"
            # aem.download_asset(config[env]["aem.sf_url"] + row["jcr:path"], des_path)
            try:
                with open(f"/home/user/develop/genai/sales/temp_data/sim_model/{file_name}.txt", "w") as f:  # "w" creates a new file or overwrites an existing one
                    f.write(f"Path: {row['jcr:path']}\n")
                    f.write(f"Created: {row['jcr:created']}\n")
                    f.write(f"Uuid: {row['jcr:uuid']}\n")
                    f.write(f"DocumentID: {row['jcr:content.mt:documentID']}\n")
                    f.write(f"DocumentSupport: {row['jcr:content.mt:documentSupport']}\n")
                    f.write(f"Names: {row['jcr:content.names']}\n")
                    f.write(f"LastModified: {row['jcr:content.jcr:lastModified']}\n")
                    f.write(f"Names-2: {row['jcr:content.names-2']}\n")
                    f.write(f"Rba: {row['jcr:content.metadata.mt:rba']}\n")
                    f.write(f"Description: {row['jcr:content.metadata.dc:description']}\n")
                    f.write(f"ContentType: {row['jcr:content.metadata.mt:contentType']}\n")
                    f.write(f"Title: {row['jcr:content.metadata.dc:title']}\n")

                    # only first 3 rows
                    items = []
                    if isinstance(row['jcr:content.metadata.dam:Content'], str):
                        items = re.split(r"[,|\n]", row['jcr:content.metadata.dam:Content'])
                    f.write(f"Content: {items[:3]}\n")
            except Exception as e:
                print(f"Error writing to file: {e}")

    except Exception as e:
        print(f"Error while fetching data: {e}")