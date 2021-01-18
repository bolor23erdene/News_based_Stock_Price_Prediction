import numpy as np
import pandas as pd



companies = ['AAPL','TSLA','TWTR','EBAY']

X = []
Y = []

for company in companies:
	company_name = '../data/' + company + '_prepared.csv'
	df_temp = pd.read_csv(company_name)
	# print(df_temp.shape)
	# print(df_temp.columns)
	#print(df_temp['title'])
	#print(df_temp['text'])
	#print('FINAL_TEXT\n\n\n\n\n')
	final_text = df_temp['title'] + '. ' + df_temp['text']
	X = np.concatenate((X,np.array(final_text)),axis=0)
	Y = np.concatenate((Y,np.array(df_temp['labels'])),axis=0)
	#Y = Y + np.array(df_temp['labels'])
	#print(final_text)
	#print(df_temp['labels'])

print(Y)

Y_new = []
for each in Y:
	if each == 0:
		Y_new.append(0)
	elif each == 1:
		Y_new.append(1)
	elif each == -1:
		Y_new.append(-1)
	else:
		Y_new.append(0)

df = pd.DataFrame({'text': X, 'labels': Y_new})
df = df.dropna()

df.to_csv('../data/datasets_joined.csv')


#X = np.reshape(X, (X.shape[0],1))
#Y = np.reshape(Y, (Y.shape[0],1))

#print(X.shape,Y.shape)#, Y.shape)
#print(X)
#print(Y)



