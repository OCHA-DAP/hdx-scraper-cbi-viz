import json
import logging
from os.path import join

import unicodecsv
from hdx.data.dataset import Dataset

logger = logging.getLogger(__name__)


def write(today, output_dir, configuration, configuration_key, rows, skipped=None):
    logger.info(f"Writing {configuration_key} files to {output_dir}")
    file_configuration = configuration[configuration_key]
    headers = file_configuration["headers"]
    hxltags = file_configuration["hxltags"]
    process_cols = file_configuration.get("process_cols", dict())
    csv_configuration = file_configuration["csv"]
    json_configuration = file_configuration["json"]
    csv_hxltags = csv_configuration.get("hxltags", hxltags)
    json_hxltags = json_configuration.get("hxltags", hxltags)
    hxltag_to_header = dict(zip(hxltags, headers))
    csv_headers = [hxltag_to_header[hxltag] for hxltag in csv_hxltags]
    metadata = {"#date+run": today, f"#meta+{configuration_key}+num": len(rows)}
    if skipped is not None:
        metadata[f"#meta+{configuration_key}+skipped+num"] = skipped
    metadata_json = json.dumps(metadata, indent=None, separators=(",", ":"))
    with open(join(output_dir, csv_configuration["filename"]), "wb") as output_csv:
        writer = unicodecsv.writer(output_csv, encoding="utf-8", lineterminator="\n")
        writer.writerow(csv_headers)
        writer.writerow(csv_hxltags)
        with open(join(output_dir, json_configuration["filename"]), "w") as output_json:
            output_json.write(f'{{"metadata":{metadata_json},"data":[\n')

            def write_row(inrow, ending):
                def get_outrow(file_hxltags):
                    outrow = dict()
                    for file_hxltag in file_hxltags:
                        expression = process_cols.get(file_hxltag)
                        if expression:
                            for i, hxltag in enumerate(hxltags):
                                expression = expression.replace(hxltag, f"inrow[{i}]")
                            outrow[file_hxltag] = eval(expression)
                        else:
                            outrow[file_hxltag] = inrow[hxltags.index(file_hxltag)]
                    return outrow

                writer.writerow(get_outrow(csv_hxltags).values())
                row = get_outrow(json_hxltags)
                output_json.write(
                    json.dumps(row, indent=None, separators=(",", ":")) + ending
                )

            [write_row(row, ",\n") for row in rows[:-1]]
            write_row(rows[-1], "]")
            output_json.write("}")


def start(configuration, today, retriever, output_dir):
    dataset = Dataset.read_from_hdx(configuration["name"])
    resource = dataset.get_resource()
    header, iterator = retriever.get_tabular_rows(resource["url"], dict_form=True)
    totals = {"overall": 0, "spending": 0, "commitments": 0}
    provider_name_to_id = dict()
    provider_count = 0
    receiver_name_to_id = dict()
    receiver_count = 0
    month = today[:7]
    flows = dict()
    transactions = list()
    for inrow in iterator:
        activity_id = int(inrow["Transaction ID"])
        provider = inrow["Private sector donor"]
        provider_id = provider_name_to_id.get(provider)
        if not provider_id:
            provider_count += 1
            provider_id = inrow["Donor ID"]
            provider_name_to_id[provider] = provider_id
        org_type = 70
        sector = inrow["Business sector"]
        receiver = inrow["Recipient"]
        receiver_id = receiver_name_to_id.get(receiver)
        if not receiver_id:
            receiver_count += 1
            receiver_id = inrow["Recipient ID"]
            receiver_name_to_id[receiver] = receiver_id
        value = float(inrow["Est USD value"])
        totals["overall"] += value
        status = inrow["Donation status"]
        if status == "paid":
            transaction = "spending"
        else:
            transaction = "commitments"
        description = inrow["Notes"]
        totals[transaction] += value
        intvalue = int(round(value))
        row = (
            month,
            provider_id,
            provider,
            org_type,
            sector,
            receiver,
            1,
            1,
            transaction,
            activity_id,
            intvalue,
            intvalue,
            description,
        )
        transactions.append(row)

        key = f"{provider_id}-{receiver_id}"
        cur_output = flows.get(key, dict())
        cur_output["value"] = cur_output.get("value", 0) + value
        if "row" not in cur_output:
            cur_output["row"] = [
                provider_id,
                provider,
                org_type,
                None,
                None,
                None,
                receiver_id,
                receiver,
                None,
                1,
                1,
                "outgoing",
            ]
            cur_output["description"] = description
        flows[key] = cur_output

    logger.info(
        f"Total: {totals['overall']}, Spending: {totals['spending']}, Commitments {totals['commitments']}"
    )
    logger.info(f"Processed {len(flows)} flows")
    logger.info(f"Processed {len(transactions)} transactions")

    outputs_configuration = configuration["outputs"]

    # Prepare and write flows
    write(
        today,
        output_dir,
        outputs_configuration,
        "flows",
        [
            flows[key]["row"]
            + [int(round(flows[key]["value"]))]
            + [flows[key]["description"]]
            for key in flows
        ],
    )

    # Write transactions
    write(
        today,
        output_dir,
        outputs_configuration,
        "transactions",
        transactions,
    )

    # Write reporting orgs
    write(
        today,
        output_dir,
        outputs_configuration,
        "reporting_orgs",
        [t[::-1] for t in sorted(provider_name_to_id.items())],
    )

    # Write receiver orgs
    write(
        today,
        output_dir,
        outputs_configuration,
        "receiver_orgs",
        [t[::-1] for t in sorted(receiver_name_to_id.items())],
    )
