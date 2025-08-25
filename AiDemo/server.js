import 'dotenv/config';
import express from 'express';
import { OpenAI } from 'openai';
import neo4j from 'neo4j-driver';

const app = express();
app.use(express.json());
app.use(express.static('public'));

// --- Neo4j driver ---
const driver = neo4j.driver(
    process.env.NEO4J_URI,
    neo4j.auth.basic(process.env.NEO4J_USER, process.env.NEO4J_PASSWORD),
    { /* encrypted: 'ENCRYPTION_OFF' for local if needed */ }
);

// --- OpenAI client ---
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// Cache for database schema
let schemaCache = null;
let schemaCacheTime = 0;
const CACHE_DURATION = 300000; // 5 minutes

// Discover actual database schema
async function discoverSchema() {
    const now = Date.now();
    if (schemaCache && (now - schemaCacheTime) < CACHE_DURATION) {
        return schemaCache;
    }

    const session = driver.session();
    try {
        // Get all labels and their counts
        const labelResult = await session.run(`
            CALL db.labels() YIELD label
            CALL {
                WITH label
                CALL apoc.cypher.run('MATCH (n:' + label + ') RETURN count(n) as count', {})
                YIELD value
                RETURN value.count as count
            }
            RETURN label, count
            ORDER BY count DESC
        `);

        // Get sample properties for each label
        const labels = {};
        for (const record of labelResult.records) {
            const label = record.get('label');
            const count = record.get('count');

            if (count > 0) {
                // Get sample properties
                const propResult = await session.run(`
                    MATCH (n:\`${label}\`)
                    WITH n LIMIT 3
                    RETURN keys(n) as props, n.nid as nid, 
                           coalesce(n.Name, n.ObjectType, n.LongName, '') as name,
                           coalesce(n.GlobalId, n.globalId, '') as globalId
                `);

                const properties = new Set();
                const samples = [];

                for (const propRecord of propResult.records) {
                    const props = propRecord.get('props') || [];
                    props.forEach(p => properties.add(p));
                    samples.push({
                        nid: propRecord.get('nid'),
                        name: propRecord.get('name'),
                        globalId: propRecord.get('globalId')
                    });
                }

                labels[label] = {
                    count,
                    properties: Array.from(properties),
                    samples: samples.slice(0, 2)
                };
            }
        }

        // Get relationship types
        const relResult = await session.run(`
            CALL db.relationshipTypes() YIELD relationshipType
            CALL {
                WITH relationshipType
                CALL apoc.cypher.run('MATCH ()-[r:\`' + relationshipType + '\`]->() RETURN count(r) as count', {})
                YIELD value
                RETURN value.count as count
            }
            RETURN relationshipType, count
            ORDER BY count DESC
        `);

        const relationships = {};
        for (const record of relResult.records) {
            const relType = record.get('relationshipType');
            const count = record.get('count');
            if (count > 0) {
                relationships[relType] = count;
            }
        }

        schemaCache = { labels, relationships };
        schemaCacheTime = now;

        return schemaCache;
    } catch (error) {
        console.warn('Error discovering schema, using fallback:', error.message);
        // Fallback schema
        return {
            labels: {
                'IfcWall': { count: 0, properties: ['nid', 'Name', 'GlobalId'], samples: [] },
                'IfcDoor': { count: 0, properties: ['nid', 'Name', 'GlobalId'], samples: [] },
                'IfcWindow': { count: 0, properties: ['nid', 'Name', 'GlobalId'], samples: [] }
            },
            relationships: {}
        };
    } finally {
        await session.close();
    }
}

// Generate dynamic schema hint based on actual database content
function generateSchemaHint(schema) {
    const { labels, relationships } = schema;

    // Create label mappings for different ways to refer to elements
    const elementMappings = {
        'wall': ['IfcWall', 'IfcWallStandardCase', 'IfcCurtainWall'],
        'door': ['IfcDoor'],
        'window': ['IfcWindow'],
        'slab': ['IfcSlab'],
        'column': ['IfcColumn'],
        'beam': ['IfcBeam'],
        'space': ['IfcSpace'],
        'room': ['IfcSpace'],
        'floor': ['IfcBuildingStorey', 'IfcSlab'],
        'storey': ['IfcBuildingStorey'],
        'building': ['IfcBuilding'],
        'site': ['IfcSite'],
        'project': ['IfcProject'],
        'pipe': ['IfcPipeSegment', 'IfcPipeFitting'],
        'duct': ['IfcDuctSegment', 'IfcDuctFitting'],
        'equipment': ['IfcDistributionElement', 'IfcFlowTerminal']
    };

    // Find actual labels that exist in the database
    const existingLabels = Object.keys(labels);
    const actualMappings = {};

    for (const [concept, possibleLabels] of Object.entries(elementMappings)) {
        const foundLabels = possibleLabels.filter(label => existingLabels.includes(label));
        if (foundLabels.length > 0) {
            actualMappings[concept] = foundLabels;
        }
    }

    // Generate the schema hint
    let hint = `You are a Cypher generator for a Neo4j graph built from an IFC file.

AVAILABLE LABELS (with counts):
${existingLabels.map(label => `- ${label} (${labels[label].count} nodes)`).join('\n')}

COMMON ELEMENT MAPPINGS:
${Object.entries(actualMappings).map(([concept, labels]) =>
        `- ${concept}: ${labels.join(' OR ')}`
    ).join('\n')}

RELATIONSHIP TYPES AVAILABLE:
${Object.keys(relationships).slice(0, 10).join(', ')}

COMMON PROPERTIES:
- nid: unique node identifier (integer)
- Name: element name (string)
- GlobalId/globalId: IFC global identifier (string)
- ObjectType: object type description (string)
- LongName: longer descriptive name (string)

QUERY GUIDELINES:
1. When user asks about "walls", use: MATCH (n) WHERE any(label IN labels(n) WHERE label CONTAINS 'Wall')
2. For counting, always use this flexible pattern for better results
3. When listing items, return useful properties: name, globalId, nid
4. Use coalesce() for name fields: coalesce(n.Name, n.ObjectType, n.LongName, 'Unnamed')
5. For spatial queries, look for IfcSpace, IfcBuildingStorey, IfcBuilding, IfcSite
6. Only output valid Cypher - no explanations
7. Use LIMIT for listing queries to avoid overwhelming results

EXAMPLES:
Q: "How many walls are there?"
A: MATCH (n) WHERE any(label IN labels(n) WHERE label CONTAINS 'Wall') RETURN count(n) AS count;

Q: "List all doors with names"
A: MATCH (n) WHERE any(label IN labels(n) WHERE label CONTAINS 'Door') RETURN coalesce(n.Name, n.ObjectType, 'Unnamed') AS name, coalesce(n.GlobalId, n.globalId, '') AS globalId, n.nid AS nid LIMIT 20;

Q: "What spaces are on the ground floor?"
A: MATCH (space) WHERE any(label IN labels(space) WHERE label CONTAINS 'Space') MATCH (floor) WHERE any(label IN labels(floor) WHERE label CONTAINS 'Storey') RETURN coalesce(space.Name, space.LongName, 'Unnamed Space') AS spaceName LIMIT 10;
`;

    return hint;
}

// Enhanced query processor with fallback strategies
async function processQuery(question, schema) {
    const session = driver.session();
    let cypher = '';
    let result = null;
    let attempts = [];

    try {
        // Generate initial Cypher using AI
        const schemaHint = generateSchemaHint(schema);
        const prompt = `${schemaHint}\nQ: "${question}"\nA:`;

        const resp = await openai.chat.completions.create({
            model: "gpt-4o-mini",
            temperature: 0.1,
            messages: [
                { role: "system", content: "Generate only valid Cypher queries for Neo4j. No explanations or markdown." },
                { role: "user", content: prompt }
            ]
        });

        cypher = (resp.choices?.[0]?.message?.content || '').trim();
        cypher = cypher.replace(/```cypher\n?/g, '').replace(/```\n?/g, '').trim();

        attempts.push({ cypher, source: 'AI' });

        // Try the AI-generated query first
        try {
            result = await session.run(cypher);
            if (result.records.length > 0) {
                return { cypher, result, attempts };
            }
        } catch (aiError) {
            console.log('AI query failed:', aiError.message);
        }

        // Fallback strategies based on question content
        const fallbacks = generateFallbackQueries(question, schema);

        for (const fallback of fallbacks) {
            attempts.push({ cypher: fallback.cypher, source: fallback.strategy });
            try {
                result = await session.run(fallback.cypher);
                cypher = fallback.cypher;
                break;
            } catch (fallbackError) {
                console.log(`Fallback ${fallback.strategy} failed:`, fallbackError.message);
            }
        }

        // Ultimate fallback - just count all nodes
        if (!result) {
            cypher = "MATCH (n) RETURN count(n) AS total_count";
            result = await session.run(cypher);
            attempts.push({ cypher, source: 'ultimate_fallback' });
        }

        return { cypher, result, attempts };

    } finally {
        await session.close();
    }
}

// Generate smart fallback queries based on question analysis
function generateFallbackQueries(question, schema) {
    const fallbacks = [];
    const lowerQuestion = question.toLowerCase();

    // Extract keywords and map to IFC concepts
    const concepts = [
        { keywords: ['wall', 'walls'], labels: ['Wall'] },
        { keywords: ['door', 'doors'], labels: ['Door'] },
        { keywords: ['window', 'windows'], labels: ['Window'] },
        { keywords: ['slab', 'slabs', 'floor', 'floors'], labels: ['Slab', 'Storey'] },
        { keywords: ['column', 'columns'], labels: ['Column'] },
        { keywords: ['beam', 'beams'], labels: ['Beam'] },
        { keywords: ['space', 'spaces', 'room', 'rooms'], labels: ['Space'] },
        { keywords: ['building', 'buildings'], labels: ['Building'] },
        { keywords: ['pipe', 'pipes'], labels: ['Pipe'] },
    ];

    for (const concept of concepts) {
        if (concept.keywords.some(keyword => lowerQuestion.includes(keyword))) {
            // Strategy 1: Flexible label matching
            const labelCondition = concept.labels
                .map(label => `label CONTAINS '${label}'`)
                .join(' OR ');

            if (lowerQuestion.includes('how many') || lowerQuestion.includes('count')) {
                fallbacks.push({
                    strategy: `count_${concept.labels[0].toLowerCase()}`,
                    cypher: `MATCH (n) WHERE any(label IN labels(n) WHERE ${labelCondition}) RETURN count(n) AS count`
                });
            } else if (lowerQuestion.includes('list') || lowerQuestion.includes('show') || lowerQuestion.includes('what')) {
                fallbacks.push({
                    strategy: `list_${concept.labels[0].toLowerCase()}`,
                    cypher: `MATCH (n) WHERE any(label IN labels(n) WHERE ${labelCondition}) RETURN coalesce(n.Name, n.ObjectType, n.LongName, 'Unnamed') AS name, coalesce(n.GlobalId, n.globalId, '') AS globalId, n.nid AS nid LIMIT 20`
                });
            }

            // Strategy 2: Try exact label matches if they exist
            for (const label of concept.labels) {
                const exactLabel = `Ifc${label}`;
                if (schema.labels[exactLabel]) {
                    if (lowerQuestion.includes('how many') || lowerQuestion.includes('count')) {
                        fallbacks.push({
                            strategy: `exact_count_${exactLabel}`,
                            cypher: `MATCH (n:\`${exactLabel}\`) RETURN count(n) AS count`
                        });
                    } else {
                        fallbacks.push({
                            strategy: `exact_list_${exactLabel}`,
                            cypher: `MATCH (n:\`${exactLabel}\`) RETURN coalesce(n.Name, n.ObjectType, 'Unnamed') AS name, coalesce(n.GlobalId, n.globalId, '') AS globalId LIMIT 10`
                        });
                    }
                }
            }
            break; // Found matching concept, don't try others
        }
    }

    // General fallbacks
    if (lowerQuestion.includes('how many') || lowerQuestion.includes('count')) {
        fallbacks.push({
            strategy: 'count_all_elements',
            cypher: "MATCH (n) WHERE any(label IN labels(n) WHERE label STARTS WITH 'Ifc') RETURN count(n) AS count"
        });
    }

    if (lowerQuestion.includes('what') || lowerQuestion.includes('show') || lowerQuestion.includes('list')) {
        fallbacks.push({
            strategy: 'show_labels',
            cypher: "MATCH (n) RETURN DISTINCT labels(n) AS node_types, count(n) AS count ORDER BY count DESC LIMIT 10"
        });
    }

    return fallbacks;
}

// Enhanced result formatter
function formatResult(result, question) {
    const records = result.records || [];

    if (records.length === 0) {
        return "No results found.";
    }

    // Single scalar result (like count)
    if (records.length === 1 && records[0].keys.length === 1) {
        const key = records[0].keys[0];
        const value = records[0].get(0);

        if (key.toLowerCase().includes('count')) {
            return `Found ${value} item(s).`;
        }
        return `Result: ${value}`;
    }

    // Multiple results or complex results
    const maxRows = 10;
    const displayRecords = records.slice(0, maxRows);

    if (records[0].keys.includes('name') || records[0].keys.includes('globalId')) {
        // Formatted list of named items
        const items = displayRecords.map(record => {
            const name = record.get('name') || 'Unnamed';
            const globalId = record.get('globalId') || '';
            const nid = record.get('nid') || '';
            return `• ${name}${globalId ? ` (ID: ${globalId})` : nid ? ` (nid: ${nid})` : ''}`;
        });

        let result = `Found ${records.length} result(s):\n${items.join('\n')}`;
        if (records.length > maxRows) {
            result += `\n... and ${records.length - maxRows} more`;
        }
        return result;
    }

    // Generic tabular format
    const rows = displayRecords.map(record => {
        const obj = {};
        record.keys.forEach(key => {
            const value = record.get(key);
            if (value && value.properties) {
                obj[key] = value.properties;
            } else if (Array.isArray(value)) {
                obj[key] = value.join(', ');
            } else {
                obj[key] = value;
            }
        });
        return obj;
    });

    return `Results (${rows.length}/${records.length}):\n` + JSON.stringify(rows, null, 2);
}

// Enhanced global ID extraction
function extractGlobalIds(records) {
    const ids = new Set();

    for (const record of records) {
        for (const key of record.keys) {
            const value = record.get(key);

            // Direct globalId fields
            if (key.toLowerCase().includes('globalid') && typeof value === 'string') {
                ids.add(value);
                continue;
            }

            // Node properties
            if (value && value.properties) {
                const globalId = value.properties.GlobalId || value.properties.globalId;
                if (globalId) ids.add(globalId);
            }

            // String that looks like IFC GUID
            if (typeof value === 'string' && /^[0-9A-Za-z_$-]{22}$/.test(value)) {
                ids.add(value);
            }
        }
    }

    return Array.from(ids);
}

// Main API endpoint
app.post('/ask', async (req, res) => {
    const { question } = req.body || {};
    if (!question) {
        return res.status(400).json({ error: 'question is required' });
    }

    try {
        // Discover current schema
        const schema = await discoverSchema();

        // Process the query with fallbacks
        const { cypher, result, attempts } = await processQuery(question, schema);

        // Format the response
        const answerText = formatResult(result, question);
        const globalIds = extractGlobalIds(result.records);

        // Add debug info in development
        const debugInfo = process.env.NODE_ENV === 'development' ? {
            schemaLabels: Object.keys(schema.labels),
            queryAttempts: attempts,
            resultCount: result.records.length
        } : undefined;

        res.json({
            question,
            cypher,
            answerText,
            globalIds,
            debug: debugInfo
        });

    } catch (error) {
        console.error('Query processing error:', error);
        res.status(500).json({
            error: 'Failed to process query',
            details: error.message,
            question
        });
    }
});

// Health check endpoint
app.get('/health', async (req, res) => {
    try {
        const session = driver.session();
        await session.run('RETURN 1');
        await session.close();
        res.json({ status: 'healthy', database: 'connected' });
    } catch (error) {
        res.status(503).json({ status: 'unhealthy', error: error.message });
    }
});

// Schema inspection endpoint
app.get('/schema', async (req, res) => {
    try {
        const schema = await discoverSchema();
        res.json(schema);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.listen(process.env.PORT || 3000, () => {
    console.log(`Enhanced IFC API listening on http://localhost:${process.env.PORT || 3000}`);
    console.log('Endpoints:');
    console.log('  POST /ask - Ask questions about your IFC model');
    console.log('  GET /schema - View database schema');
    console.log('  GET /health - Health check');
});