import boto3
import json
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Global Dry-Run Mode
DRY_RUN = False  # Set to False to apply changes


def get_ec2_client(region):
    """Returns an EC2 client for the specified region."""
    return boto3.client("ec2", region_name=region)


def tags_has_ci_igw(tags):
    for tag in tags:
        if tag["Key"] == "ci-igw" and tag["Value"].lower() == "true":
            return True
    return False


def is_valid_ci_vpc(vpc_id, ec2_client):
    """Check if the VPC has the required tags:
       - 'ci-igw=true'
    """
    response = ec2_client.describe_tags(Filters=[
        {"Name": "resource-id", "Values": [vpc_id]}
    ])
    return tags_has_ci_igw(response.get("Tags", []))


def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")

    detail = event.get("detail", {})
    event_name = detail.get("eventName", None)
    ec2_state = detail.get("state", None)

    if "errorCode" in detail or "errorMessage" in detail:
        logger.warning(f"Skipping event {event_name} due to API error: {detail.get('errorMessage', 'Unknown error')}")
        return

    if event_name == "CreateSubnet":
        subnet_id = detail["responseElements"]["subnet"]["subnetId"]
        vpc_id = detail["responseElements"]["subnet"]["vpcId"]
        region = detail["awsRegion"]

        subnet_data = detail["responseElements"]["subnet"]
        subnet_tags = subnet_data.get("tags", [])

        if not tags_has_ci_igw(subnet_tags):
            logger.info(f"Skipping subnet {subnet_id} (VPC {vpc_id} does not meet required tags)")
            return

        ec2_client = get_ec2_client(region)

        if DRY_RUN:
            logger.info(f"DRY RUN: Would modify subnet {subnet_id} to map public IPs")
        else:
            ec2_client.modify_subnet_attribute(
                SubnetId=subnet_id,
                MapPublicIpOnLaunch={"Value": True}
            )
            logger.info(f"Modified subnet {subnet_id} to map public IPs")

    elif event_name == "CreateRoute":
        route_table_id = detail["requestParameters"]["routeTableId"]
        destination_cidr = detail["requestParameters"]["destinationCidrBlock"]

        if destination_cidr != "0.0.0.0/0":
            return  # Ignore non-default routes

        region = detail["awsRegion"]
        ec2_client = get_ec2_client(region)

        # Get the VPC ID from the Route Table
        route_table_info = ec2_client.describe_route_tables(RouteTableIds=[route_table_id])
        vpc_id = route_table_info["RouteTables"][0]["VpcId"]

        if is_valid_ci_vpc(vpc_id, ec2_client):
            logger.info(f"Default route created in qualifying VPC {vpc_id}, Route Table: {route_table_id}")

            # Find the Internet Gateway (IGW)
            igw_id = find_igw_for_vpc(vpc_id, ec2_client)
            if not igw_id:
                return  # No IGW, skip further actions

            # Find all subnet route tables in the VPC
            route_tables = find_subnet_route_tables(vpc_id, ec2_client)

            # Ensure all subnets route 0.0.0.0/0 to the IGW
            ensure_igw_routing(route_tables, igw_id, ec2_client)
        else:
            logger.info(f"Skipping event. VPC {vpc_id} does not meet required tags")

    elif ec2_state == "running" or ec2_state == "terminated":
        # State change are slightly different from API call events:
        # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/monitoring-instance-state-changes.html
        region = event["region"]
        ec2_client = get_ec2_client(region)
        instance_id = event['detail']['instance-id']

        if ec2_state == "running":
            handle_instance_running(instance_id, ec2_client)

        if ec2_state == "terminated":
            handle_instance_terminated(instance_id, ec2_client)


def find_igw_for_vpc(vpc_id, ec2_client):
    """Find the Internet Gateway (IGW) attached to the given VPC."""
    igws = ec2_client.describe_internet_gateways(
        Filters=[{"Name": "attachment.vpc-id", "Values": [vpc_id]}]
    ).get("InternetGateways", [])

    if not igws:
        logger.warning(f"No IGW found for VPC {vpc_id}")
        return None

    return igws[0]["InternetGatewayId"]


def find_subnet_route_tables(vpc_id, ec2_client):
    """Find all route tables associated with subnets in the VPC."""
    route_tables = ec2_client.describe_route_tables(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    ).get("RouteTables", [])

    return {rt["RouteTableId"]: rt for rt in route_tables}


def ensure_igw_routing(route_tables, igw_id, ec2_client):
    """Ensure each route table in the VPC routes 0.0.0.0/0 to the IGW."""
    for route_table_id, rt in route_tables.items():
        # Check if a route for 0.0.0.0/0 already exists pointing to IGW
        for route in rt.get("Routes", []):
            if route.get("DestinationCidrBlock") == "0.0.0.0/0":
                if route.get("GatewayId") == igw_id:
                    logger.info(f"Route table {route_table_id} already routes 0.0.0.0/0 to IGW {igw_id}, skipping.")
                    continue  # No update needed

        # If not, add the route
        if DRY_RUN:
            logger.info(f"DRY RUN: Would update route table {route_table_id} to route 0.0.0.0/0 to IGW {igw_id}")
        else:
            ec2_client.replace_route(
                RouteTableId=route_table_id,
                DestinationCidrBlock="0.0.0.0/0",
                GatewayId=igw_id
            )
            logger.info(f"Updated route table {route_table_id} to route 0.0.0.0/0 to IGW {igw_id}")


def handle_instance_running(instance_id, ec2_client):
    """ Assigns an Elastic IP if the instance does not have a public IP """
    print(f"Checking instance {instance_id} for public IP...")

    reservations = ec2_client.describe_instances(InstanceIds=[instance_id])
    instance = reservations["Reservations"][0]["Instances"][0]
    tags = instance.get('Tags', [])

    has_ci_igw = False
    for tag in tags:
        if tag["Key"] == "ci-igw" and tag["Value"].lower() == "true":
            has_ci_igw = True
            break

    if not has_ci_igw:
        print(f"Instance {instance_id} does not have the tag 'ci-igw=true'. Skipping EIP association.")
        return

    # Check if instance has a public IP
    network_interfaces = instance.get("NetworkInterfaces", [])
    has_public_ip = any(ni.get("Association", {}).get("PublicIp") for ni in network_interfaces)

    if not has_public_ip:
        print(f"Instance {instance_id} does not have a public IP. Allocating EIP...")

        if DRY_RUN:
            print(f"DRY_RUN: Would allocate EIP and associate it with instance {instance_id}")
            return

        allocate_response = ec2_client.allocate_address(Domain="vpc")
        allocation_id = allocate_response["AllocationId"]

        # Associate EIP with instance
        ec2_client.associate_address(InstanceId=instance_id, AllocationId=allocation_id)

        # Tag the EIP with the instance ID for tracking
        ec2_client.create_tags(
            Resources=[allocation_id],
            Tags=[{"Key": "AssociatedCiIgwInstance", "Value": instance_id}]
        )

        print(f"Successfully assigned EIP {allocate_response['PublicIp']} to instance {instance_id}")
    else:
        print(f"Instance {instance_id} already has a public IP, skipping assignment.")


def handle_instance_terminated(instance_id, ec2_client):
    """ Releases the EIP associated with a terminated instance """
    print(f"Checking for EIP associated with terminated instance {instance_id}...")

    # The VPC and any other resources associated with the instance may already be gone,
    # so the best way to find the EIP is to search using the tag we applied when
    # we applied it.
    addresses = ec2_client.describe_addresses(Filters=[{"Name": "tag:AssociatedCiIgwInstance", "Values": [instance_id]}])

    for address in addresses.get("Addresses", []):

        association_id = address.get("AssociationId")
        # If the EIP is still associated, attempt to disassociate it first
        if association_id:
            print(f"EIP {allocation_id} is still associated (AssociationId: {association_id}). Disassociating...")
            try:
                ec2_client.disassociate_address(AssociationId=association_id)
            except Exception as e:
                print(f"Unable to disassociate EIP {allocation_id}: {e}")

        allocation_id = address["AllocationId"]
        public_ip = address["PublicIp"]

        # Release the EIP
        ec2_client.release_address(AllocationId=allocation_id)
        print(f"Released EIP {public_ip} (Allocation ID: {allocation_id}) for terminated instance {instance_id}")