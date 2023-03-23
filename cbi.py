import logging

from hdx.data.dataset import Dataset
from hdx.utilities.saver import save_hxlated_output
from hdx.utilities.text import get_numeric_if_possible

logger = logging.getLogger(__name__)


def start(configuration, today, retriever, output_dir, whattorun):
    dataset = Dataset.read_from_hdx(configuration[f"dataset_{whattorun}"])
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
        activity_id = str(inrow["Transaction ID"])
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
        value = get_numeric_if_possible(inrow["Est USD value"])
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
            receiver_id,
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

    # def write(today, output_dir, configuration, configuration_key, rows, skipped=None):
    # Prepare and write flows
    logger.info(f"Writing flows files to {output_dir}")
    out_flows = [
        flows[key]["row"]
        + [int(round(flows[key]["value"]))]
        + [flows[key]["description"]]
        for key in flows
    ]
    save_hxlated_output(
        outputs_configuration["flows"],
        out_flows,
        includes_header=False,
        includes_hxltags=False,
        output_dir=output_dir,
        today=today,
        num_flows=len(out_flows),
    )

    # Write transactions
    logger.info(f"Writing transactions files to {output_dir}")
    save_hxlated_output(
        outputs_configuration["transactions"],
        transactions,
        includes_header=False,
        includes_hxltags=False,
        output_dir=output_dir,
        today=today,
        num_transactions=len(transactions),
    )

    # Write reporting orgs
    logger.info(f"Writing reporting orgs files to {output_dir}")
    reporting_orgs = [t[::-1] for t in sorted(provider_name_to_id.items())]
    save_hxlated_output(
        outputs_configuration["reporting_orgs"],
        reporting_orgs,
        includes_header=False,
        includes_hxltags=False,
        output_dir=output_dir,
        today=today,
        num_reporting_orgs=len(reporting_orgs),
    )

    # Write receiver orgs
    logger.info(f"Writing receiver orgs files to {output_dir}")
    receiver_orgs = [t[::-1] for t in sorted(receiver_name_to_id.items())]
    save_hxlated_output(
        outputs_configuration["receiver_orgs"],
        receiver_orgs,
        includes_header=False,
        includes_hxltags=False,
        output_dir=output_dir,
        today=today,
        num_receiver_orgs=len(receiver_orgs),
    )
