
Out = {"TC_ID":[], "test_Case_Name":[], "Number_of_source_Records":[],
       "Number_of_target_Records":[], "Number_of_failed_Records":[],"Status":[]}
schema= ["TC_ID", "test_Case_Name", "Number_of_source_Records",
         "Number_of_target_Records", "Number_of_failed_Records","Status"]



def write_output(TC_ID,Test_Case_Name,Number_of_source_Records,
                 Number_of_target_Records,Status,Number_of_failed_Records,Out):
    Out["TC_ID"].append(TC_ID)
    Out["test_Case_Name"].append(Test_Case_Name)
    Out["Number_of_source_Records"].append(Number_of_source_Records)
    Out["Number_of_target_Records"].append(Number_of_target_Records)
    Out["Status"].append(Status)
    Out["Number_of_failed_Records"].append(Number_of_failed_Records)

#This is pull request verification

def write_output2(TC_ID,TC_ID2,Test_Case_Name,Number_of_source_Records,
                 Number_of_target_Records,Status,Number_of_failed_Records,Out):
    Out["TC_ID"].append(TC_ID)
    Out["TC_ID2"].append(TC_ID2)

def write_output3(TC_ID,Test_Case_Name,Number_of_source_Records,
                 Number_of_target_Records,Status,Number_of_failed_Records,Out):
    Out["TC_ID"].append(TC_ID)
