import json

# -------- CONFIG --------
input_file = r"C:\Users\MC823AX\Downloads\cities500.json"     # Your big JSON file
output_file = r"mp_only.json"  # Filtered output file
TARGET_STATE = "Madhya Pradesh"
# ------------------------

def main():
    # Load JSON data
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Ensure it's a list
    if not isinstance(data, list):
        raise ValueError("JSON root must be a list of objects")

    print(f"Total records in input file: {len(data)}")

    # Filter records
    filtered = [
        obj for obj in data
        if obj.get("admin1") == TARGET_STATE
    ]

    print(f"Records found for {TARGET_STATE}: {len(filtered)}")

    # Save to new file
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(filtered, f, indent=4, ensure_ascii=False)

    print(f"Filtered data saved to: {output_file}")


if __name__ == "__main__":
    main()
