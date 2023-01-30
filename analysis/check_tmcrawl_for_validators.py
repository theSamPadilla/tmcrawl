import json

#Open JSONs
with open("info/tmcrawl_result.json") as tm, open("info/tendermint_validators.json") as va, open("info/cosmos_validators.json") as op:
    tmcrawl = json.load(tm)
    validators = json.load(va)
    operators = json.load(op)
    tm.close()
    va.close()
    op.close()

#Iterate through tmcrawl results and clean them
seen_addresses_to_IP = {}
clean_nodes = []
nodes_with_id = []
nodes_with_address = []

for node in tmcrawl['nodes']:
    #Catch empty IP Addresses
    if node["ip"] == "":
        continue

    #Add validator if address exists
    if node["validator_address"] != "":
        ip = node["ip"]
        address = node["validator_address"]
        node_id = node["id"]
        voting_power = node ["validator_voting_power"]
        
        seen_addresses_to_IP[address] = {
            "ip": ip,
            "id": node_id,
            "voting_power": voting_power
        }

        nodes_with_address.append(node)

    if node["id"] != "":
        nodes_with_id.append(node)

    #Add validator to clean nodes
    clean_nodes.append(node)



#Write clean tmcrawl
json_object1 = json.dumps(clean_nodes, indent=4)
json_object2 = json.dumps(nodes_with_id, indent=4)
json_object3 = json.dumps(nodes_with_address, indent=4)
 
# Writing json
with open("results/clean_tmcrawl_results.json", "w") as out1, open("results/nodes_with_id_tmcrawl.json", "w") as out2, open("results/nodes_with_address_tmcrawl.json", "w") as out3:
    out1.write(json_object1)
    out2.write(json_object2)
    out3.write(json_object3)
    out1.close()
    out2.close()
    out3.close()

#Iterate through the list of validators
result = {}
val_oper_adree_to_info = {v["pub_key"]["value"]: v for v in operators["result"]["validators"]}
for val in validators["result"]["validators"]:
    addy = val["address"]
    #Catch seen validators, add pubkey info and operator addy
    if addy in seen_addresses_to_IP:
        consensus_key = val["pub_key"]["value"]
        seen_addresses_to_IP[addy]["pub_key"] = consensus_key 
        
        #Nest under try/catch for keys not seen
        try:
            seen_addresses_to_IP[addy]["validator_operator_address"] = val_oper_adree_to_info[consensus_key]["address"]
        except KeyError:
            pass

        #Write final results
        result[addy] = seen_addresses_to_IP[addy]

#Save and Print results
json_result = json.dumps(result, indent=4)
 
# Writing json
with open("results/identified_top175_validators.json", "w") as out:
    out.write(json_result)
    out.close()

print("\nFound %d active validators in tmcrawl." % len(result))
print("Cleaned tmcrawl results from %d to only %d nodes/IPs (-%d)." % (len(tmcrawl['nodes']), len(clean_nodes), len(tmcrawl['nodes'])-len(clean_nodes)))
print("Found %d nodes with IP and ID" % len(nodes_with_id))
print("Found %d nodes with IP and Address" % len(nodes_with_address))
print("\nThese validators are:")
for k, v in result.items():
    print("%s:\n%s" % (k, v))
    print()