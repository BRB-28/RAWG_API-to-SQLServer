import subprocess

# Step 1: Run the S3 upload script
print("Running API → S3 script...")
subprocess.run(['python', 'RawgAPI_to_AWS.py'])

# Step 2: Run the transform + SQL load script
print("Running transform + SQL load script...")
subprocess.run(['python', 'TransformJSON.py'])

print("Pipeline complete.")
