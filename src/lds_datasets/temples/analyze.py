import json


def main():
    # read in monthly_data and respond with top and bottom for sessions and capacity
    # for each ordinance type
    with open("temples/monthly_data.json", "r") as f:
        data = json.load(f)

    # Create a dictionary to store sorted lists for each metric and ordinance type
    sorted_lists = {}

    # Loop through each ordinance type
    for ordinance_type in data[
        next(iter(data))
    ]:  # Assuming all temple entries have the same ordinance types
        # Create a dictionary to store sorted lists for each metric
        sorted_lists[ordinance_type] = {}

        # Loop through each metric
        for metric in data[next(iter(data))][ordinance_type]:
            # Flatten the nested dictionary into a list of tuples
            flat_data = []

            for temple_name, temple_data in data.items():
                # Skip zero values
                if ordinance_type not in temple_data:
                    continue

                metric_value = temple_data[ordinance_type][metric]
                if metric_value != 0:
                    flat_data.append((temple_name, metric_value))

            # Sort the list of tuples by the metric values
            sorted_data = sorted(flat_data, key=lambda item: item[1])

            # Store the sorted list in the sorted_lists dictionary
            sorted_lists[ordinance_type][metric] = sorted_data

    # Output the sorted lists to a JSON file
    with open("temples/sorted_lists.json", "w") as json_file:
        json.dump(sorted_lists, json_file, indent=4)


if __name__ == "__main__":
    main()
