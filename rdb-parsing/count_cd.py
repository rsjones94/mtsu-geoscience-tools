
data_file = r'C:\Users\rj3h\Desktop\rdb\dv2.txt'

with open(data_file) as f:
    content = f.readlines()

count = 0
for i, line in enumerate(content):
    if 'agency_cd' in line:
        count += 1
        print(i)

print(count)
