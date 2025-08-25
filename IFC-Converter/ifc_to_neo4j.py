import ifcopenshell
import sys
from py2neo import Graph, Node
import time
import os


def typeDict(key, source_file=None):
    """Get attribute names for an IFC entity type"""
    try:
        # Try to use the same schema version as the source file if available
        if source_file:
            temp_file = ifcopenshell.file(schema=source_file.schema)
        else:
            temp_file = ifcopenshell.file()
        
        entity = temp_file.create_entity(key)
        return entity.wrapped_data.get_attribute_names()
    except RuntimeError as e:
        if "not found in schema" in str(e):
            print(f"Warning: Entity type {key} not found in schema, using generic attribute names", file=sys.stderr)
            # Return generic attribute names based on position
            return [f"attr_{i}" for i in range(20)]  # Reasonable default
        else:
            print(f"Error creating entity {key}: {e}", file=sys.stderr)
            return []
    except Exception as e:
        print(f"Unexpected error with entity {key}: {e}", file=sys.stderr)
        return []


def sanitize_neo4j_identifier(identifier):
    """Sanitize identifiers for Neo4j (remove special characters)"""
    # Replace special characters that Neo4j doesn't allow in labels/property names
    import re
    return re.sub(r'[^a-zA-Z0-9_]', '_', str(identifier))


start = time.time()  # Calculate time to process

model_name = "ifcbridge"
ifc_path = "ifc_files/" + model_name + ".ifc"

# Check if file exists
if not os.path.exists(ifc_path):
    print(f"Error: IFC file not found at {ifc_path}", file=sys.stderr)
    sys.exit(1)

start = time.time()  # Calculate time to process
print("Start!")
print(time.strftime("%Y/%m/%d %H:%M:%S", time.localtime()))
log1 = str(time.strftime("%Y/%m/%d %H:%M:%S", time.localtime())) + " Start "

nodes = []
edges = []

try:
    f = ifcopenshell.open(ifc_path)
    print(f"Opened IFC file with schema: {f.schema}")
except Exception as e:
    print(f"Error opening IFC file: {e}", file=sys.stderr)
    sys.exit(1)

for el in f:
    if el.is_a() == "IfcOwnerHistory":
        continue
    
    tid = el.id()
    cls = el.is_a()
    pairs = []
    keys = []
    
    try:
        keys = [x for x in el.get_info() if x not in ["type", "id", "OwnerHistory"]]
    except RuntimeError:
        # Handle runtime errors when getting entity info
        print(f"RuntimeError getting info for entity {tid}", file=sys.stderr)
        continue
    
    for key in keys:
        try:
            val = el.get_info()[key]
            if val is None:
                continue
                
            # Handle IFC value types
            if any(hasattr(val, "is_a") and val.is_a(thisTyp)
                   for thisTyp in ["IfcBoolean", "IfcLabel", "IfcText", "IfcReal"]):
                val = val.wrappedValue
            
            # Handle tuples of basic types
            if val and type(val) is tuple and len(val) > 0 and type(val[0]) in (str, bool, float, int):
                val = ",".join(str(x) for x in val)
            
            # Only keep basic types that Neo4j can handle
            if type(val) not in (str, bool, float, int):
                continue
                
            pairs.append((key, val))
        except Exception as e:
            print(f"Error processing property {key} for entity {tid}: {e}", file=sys.stderr)
            continue
    
    nodes.append((tid, cls, pairs))

    # Process relationships - pass the source file for schema consistency
    try:
        entity_attributes = typeDict(cls, f)  # Pass the source file
    except Exception as e:
        print(f"Error getting type dictionary for {cls}: {e}", file=sys.stderr)
        entity_attributes = []
    
    for i in range(len(el)):
        try:
            attr_value = el[i]
        except (RuntimeError, IndexError) as e:
            if "Entity not found" not in str(e):
                print(f"ID {tid}, attribute {i}: {e}", file=sys.stderr)
            continue
        
        # Get attribute name safely
        attr_name = entity_attributes[i] if i < len(entity_attributes) else f"attr_{i}"
        attr_name = sanitize_neo4j_identifier(attr_name)
        
        # Handle single entity instances
        if isinstance(attr_value, ifcopenshell.entity_instance):
            if attr_value.is_a() == "IfcOwnerHistory":
                continue
            if attr_value.id() != 0:
                edges.append((tid, cls, attr_value.id(), attr_value.is_a(), attr_name))
                continue
        
        # Handle collections of entity instances
        try:
            # Check if it's iterable
            iter(attr_value)
            destinations = []
            destinations_cls = []
            
            for x in attr_value:
                if isinstance(x, ifcopenshell.entity_instance):
                    destinations.append(x.id())
                    destinations_cls.append(x.is_a())
            
            for (connectedTo, connectedTo_cls) in zip(destinations, destinations_cls):
                edges.append((tid, cls, connectedTo, connectedTo_cls, attr_name))
                
        except TypeError:
            # Not iterable, skip
            continue
        except Exception as e:
            print(f"Error processing collection for entity {tid}, attribute {i}: {e}", file=sys.stderr)
            continue

if len(nodes) == 0:
    print("No nodes in file", file=sys.stderr)
    sys.exit(1)

print(f"List creation process done. Took {round(time.time() - start)} seconds")
print(time.strftime("%Y/%m/%d %H:%M:%S", time.localtime()))
log2 = f"{round(time.time() - start)}sec.\n{time.strftime('%Y/%m/%d %H:%M:%S', time.localtime())} List creation process done"

# Initialize neo4j database
try:
    graph = Graph(
        "neo4j://127.0.0.1:7687",
        auth=("neo4j", "azeazeazeaze")
    )
    
    # Test connection
    graph.run("RETURN 1")
    
except Exception as e:
    print(f"Error connecting to Neo4j: {e}", file=sys.stderr)
    sys.exit(1)

# Clear existing data
try:
    graph.delete_all()
except Exception as e:
    print(f"Error clearing database: {e}", file=sys.stderr)
    sys.exit(1)

# Create nodes
created_nodes = 0
for node in nodes:
    try:
        nId, cls, pairs = node
        sanitized_cls = sanitize_neo4j_identifier(cls)
        one_node = Node(sanitized_cls, nid=nId, modelId=model_name)
        
        for k, v in pairs:
            # Sanitize property names
            sanitized_key = sanitize_neo4j_identifier(k)
            one_node[sanitized_key] = v
            
        graph.create(one_node)
        created_nodes += 1
        
    except Exception as e:
        print(f"Error creating node {nId}: {e}", file=sys.stderr)
        continue

print(f"Node creation process done. Created {created_nodes} nodes. Took {round(time.time() - start)} seconds")
print(time.strftime("%Y/%m/%d %H:%M:%S", time.localtime()))

# Create relationships
query_rel = """
MATCH (a:{cls1})
WHERE a.nid = $id1
MATCH (b:{cls2})
WHERE b.nid = $id2
CREATE (a)-[:{relType} {{modelId: $modelId}}]->(b)
"""

created_edges = 0
for (id1, cls1, id2, cls2, relType) in edges:
    try:
        # Sanitize class names and relationship type
        sanitized_cls1 = sanitize_neo4j_identifier(cls1)
        sanitized_cls2 = sanitize_neo4j_identifier(cls2)
        sanitized_relType = sanitize_neo4j_identifier(relType)
        
        formatted_query = query_rel.format(
            cls1=sanitized_cls1, 
            cls2=sanitized_cls2, 
            relType=sanitized_relType
        )
        
        graph.run(formatted_query, id1=id1, id2=id2, modelId=model_name)
        created_edges += 1
        
    except Exception as e:
        print(f"Error creating relationship {id1} -> {id2}: {e}", file=sys.stderr)
        continue

total_time = round(time.time() - start)
print(f"All done. Created {created_nodes} nodes and {created_edges} relationships. Took {total_time} seconds")
print(time.strftime("%Y/%m/%d %H:%M:%S", time.localtime()))

log3 = f"{total_time}sec.\n{time.strftime('%Y/%m/%d %H:%M:%S', time.localtime())} All done"

# Write log file
try:
    with open("log.txt", mode="a", encoding="utf-8") as f:
        f.write(f"{ifc_path}\n")
        f.write(f"Nodes_{len(nodes)}, Edges_{len(edges)}\n")
        f.write(f"Created_Nodes_{created_nodes}, Created_Edges_{created_edges}\n")
        f.write(f"{log1}\n")
        f.write(f"{log2}\n")
        f.write(f"{log3}\n\n")
except Exception as e:
    print(f"Error writing log file: {e}", file=sys.stderr)

print("Process completed successfully!")