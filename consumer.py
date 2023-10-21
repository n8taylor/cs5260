import sys

n = len(sys.argv)
if n != 7:
    print("""
Command-line arguments:
        -rb             Name of bucket that will contain requests
        -st             Type of request storage [s3, dyamodb]
        -sn             Name of storage resource
""")
for i in range(1, n):
	print(sys.argv[i], end = " ")
