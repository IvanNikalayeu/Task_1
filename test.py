df_str = ('data/rooms.json')
ind_dot = df_str.find('.')
ind_sl = df_str.rfind('/')
print(ind_dot)
print(ind_sl)
name_table = df_str[ind_sl+1:ind_dot]
print(name_table)
