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

import pandas as pd
import cdsw

# args = {"feature" : "US,2040,DCA,BOS,1630,81,399,1,16"}

from joblib import dump, load

ct = load("models/ct.joblib")
pipe = load("models/pipe.joblib")


@cdsw.model_metrics
def predict_cancelled(args):
    inputs = args["feature"].split(",")
    inputs[1] = int(inputs[1])
    inputs[4] = int(inputs[4])
    inputs[5] = int(inputs[5])
    inputs[6] = int(inputs[6])
    inputs[7] = int(inputs[7])
    inputs[8] = int(inputs[8])

    input_cols = [
        "OP_CARRIER",
        "OP_CARRIER_FL_NUM",
        "ORIGIN",
        "DEST",
        "CRS_DEP_TIME",
        "CRS_ELAPSED_TIME",
        "DISTANCE",
        "WEEK",
        "HOUR",
    ]
    input_df = pd.DataFrame([inputs], columns=input_cols)

    input_transformed = ct.transform(input_df)

    prediction = pipe.predict(input_transformed)
    cdsw.track_metric("input_data", args)
    cdsw.track_metric("prediction", int(prediction[0]))

    return {"prediction": int(prediction[0])}
