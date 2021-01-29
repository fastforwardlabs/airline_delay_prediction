# ###########################################################################
#
#  CLOUDERA APPLIED MACHINE LEARNING PROTOTYPE (AMP)
#  (C) Cloudera, Inc. 2021
#  All rights reserved.
#
#  Applicable Open Source License: Apache 2.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# ###########################################################################

import cdsw, time, os, random, json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import classification_report
from cmlbootstrap import CMLBootstrap
import seaborn as sns
import copy
import time


## Set the model ID from deployed model

flights_data_df = pd.read_csv("data/all_flight_data_spark.csv")

# Get the various Model CRN details
HOST = os.getenv("CDSW_API_URL").split(":")[0] + "://" + os.getenv("CDSW_DOMAIN")
USERNAME = os.getenv("CDSW_PROJECT_URL").split("/")[6]  # args.username  # "vdibia"
API_KEY = os.getenv("CDSW_API_KEY")
PROJECT_NAME = os.getenv("CDSW_PROJECT")

cml = CMLBootstrap(HOST, USERNAME, API_KEY, PROJECT_NAME)

model_id = "33"

latest_model = cml.get_model(
    {"id": model_id, "latestModelDeployment": True, "latestModelBuild": True}
)

Model_CRN = latest_model["crn"]
Deployment_CRN = latest_model["latestModelDeployment"]["crn"]


while True:
    predicted_result = []
    actual_result = []
    start_time_ms = int(round(time.time() * 1000))
    for i in range(100):
        input_data = flights_data_df.sample(n=1)[
            [
                "OP_CARRIER",
                "OP_CARRIER_FL_NUM",
                "ORIGIN",
                "DEST",
                "CRS_DEP_TIME",
                "CRS_ELAPSED_TIME",
                "DISTANCE",
                "HOUR",
                "WEEK",
                "CANCELLED",
            ]
        ].to_numpy()[0]

        try:

            input_data[1] = int(input_data[1])
            input_data[4] = int(input_data[4])
            input_data[5] = int(input_data[5])
            input_data[6] = int(input_data[6])
            input_data[9] = int(input_data[9])

            input_data_string = ""
            for record in input_data[:-1]:
                input_data_string = input_data_string + str(record) + ","

            input_data_string = input_data_string[:-1]
            response = cdsw.call_model(
                latest_model["accessKey"], {"feature": input_data_string}
            )

            predicted_result.append(response["response"]["prediction"]["prediction"])
            actual_result.append(input_data[-1:][0])
            cdsw.track_delayed_metrics(
                {"actual_result": input_data[-1:][0]}, response["response"]["uuid"]
            )
            print(str(i) + " adding " + input_data_string)
        except:
            print("invalid row")
        time.sleep(0.2)
    end_time_ms = int(round(time.time() * 1000))
    accuracy = classification_report(actual_result, predicted_result, output_dict=True)[
        "accuracy"
    ]
    cdsw.track_aggregate_metrics(
        {"accuracy": accuracy},
        start_time_ms,
        end_time_ms,
        model_deployment_crn=Deployment_CRN,
    )
    print("adding accuracy measure of" + str(accuracy))
