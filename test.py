from lambda_function import lambda_handler

event = "{'key1' : 'value1'}"

print(lambda_handler(event=event, context=None))