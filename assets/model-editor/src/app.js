// Model Editor Application
class ModelEditor {
    constructor() {
        this.canvas = null;
        this.svg = null;
        this.width = 0;
        this.height = 0;
        this.nodes = [];
        this.links = [];
        this.selectedNode = null;
        this.nodeIdCounter = 0;
        this.linkIdCounter = 0;
        this.isDraggingNode = false;
        this.dragOffset = { x: 0, y: 0 };
        
        this.init();
        this.setupEventListeners();
    }
    
    init() {
        const canvasArea = d3.select('#canvas-area');
        const svg = d3.select('#canvas-svg');
        
        // Set up dimensions
        this.width = canvasArea.node().getBoundingClientRect().width;
        this.height = canvasArea.node().getBoundingClientRect().height;
        
        svg.attr('width', this.width).attr('height', this.height);
        
        // Add arrow marker
        svg.append('defs').selectAll('marker')
            .data(['arrow'])
            .enter()
            .append('marker')
            .attr('id', 'arrow')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 25)
            .attr('refY', 0)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .append('path')
            .attr('d', 'M0,-5L10,0L0,5');
        
        this.canvas = canvasArea;
        this.svg = svg;
    }
    
    setupEventListeners() {
        // Toolbox item clicks
        d3.selectAll('.toolbox-item[data-type]').on('click', (event) => {
            const type = d3.select(event.currentTarget).attr('data-type');
            this.createNode(type);
        });
        
        // Template clicks
        d3.selectAll('.toolbox-item[data-template]').on('click', (event) => {
            const template = d3.select(event.currentTarget).attr('data-template');
            this.loadTemplate(template);
        });
        
        // Button clicks
        d3.select('#save-btn').on('click', () => this.saveModel());
        d3.select('#validate-btn').on('click', () => this.validateModel());
        d3.select('#clear-btn').on('click', () => this.clearCanvas());
        d3.select('#export-btn').on('click', () => this.exportJSON());
        
        // Canvas interactions
        this.canvas.on('click', (event) => {
            if (!this.isDraggingNode) {
                this.deselectAll();
            }
        });
        
        // Window resize
        window.addEventListener('resize', () => {
            this.resize();
        });
    }
    
    createNode(type, x = 100, y = 100, name = null) {
        const nodeId = `node-${++this.nodeIdCounter}`;
        const nodeName = name || `${type}_${this.nodeIdCounter}`;
        
        const node = {
            id: nodeId,
            type: type,
            name: nodeName,
            x: x,
            y: y,
            properties: {},
            relations: []
        };
        
        this.nodes.push(node);
        this.renderNode(node);
        return node;
    }
    
    renderNode(node) {
        const g = this.svg.append('g')
            .attr('class', `canvas-node ${node.type}`)
            .attr('id', node.id)
            .attr('transform', `translate(${node.x}, ${node.y})`)
            .call(d3.drag()
                .on('start', (event) => this.dragNodeStarted(event, node))
                .on('drag', (event) => this.dragNode(event, node))
                .on('end', (event) => this.dragNodeEnded(event, node)));
        
        // Node rectangle
        g.append('rect')
            .attr('width', 150)
            .attr('height', 80)
            .attr('rx', 8)
            .attr('ry', 8)
            .style('fill', 'white')
            .style('stroke', '#3498db')
            .style('stroke-width', 2);
        
        // Node type
        g.append('text')
            .attr('class', 'node-type')
            .attr('x', 75)
            .attr('y', 25)
            .attr('text-anchor', 'middle')
            .text(node.type)
            .style('font-size', '12px');
        
        // Node name
        g.append('text')
            .attr('class', 'node-name')
            .attr('x', 75)
            .attr('y', 50)
            .attr('text-anchor', 'middle')
            .text(node.name)
            .style('font-size', '16px');
        
        // Click event for selection
        g.on('click', (event) => {
            event.stopPropagation();
            this.selectNode(node);
        });
        
        return g;
    }
    
    createLink(sourceNode, targetNode, relationType = 'related') {
        const linkId = `link-${++this.linkIdCounter}`;
        
        const link = {
            id: linkId,
            source: sourceNode.id,
            target: targetNode.id,
            relation: relationType,
            constraints: []
        };
        
        this.links.push(link);
        this.renderLink(link);
        return link;
    }
    
    renderLink(link) {
        const sourceNode = this.nodes.find(n => n.id === link.source);
        const targetNode = this.nodes.find(n => n.id === link.target);
        
        if (!sourceNode || !targetNode) return;
        
        const line = this.svg.append('line')
            .attr('class', 'canvas-link')
            .attr('id', link.id)
            .attr('x1', sourceNode.x + 75)
            .attr('y1', sourceNode.y + 80)
            .attr('x2', targetNode.x + 75)
            .attr('y2', targetNode.y)
            .attr('marker-end', 'url(#arrow)');
        
        // Add relation label
        const midX = (sourceNode.x + targetNode.x) / 2 + 75;
        const midY = (sourceNode.y + targetNode.y) / 2 + 40;
        
        const label = this.svg.append('text')
            .attr('class', 'link-label')
            .attr('x', midX)
            .attr('y', midY - 10)
            .attr('text-anchor', 'middle')
            .text(link.relation)
            .style('font-size', '12px')
            .style('fill', '#333')
            .style('background-color', 'white');
        
        return line;
    }
    
    selectNode(node) {
        // Deselect previous
        this.deselectAll();
        
        // Select new node
        this.selectedNode = node;
        d3.select(`#${node.id}`).classed('selected', true);
        
        // Show properties
        this.showNodeProperties(node);
    }
    
    deselectAll() {
        this.selectedNode = null;
        d3.selectAll('.canvas-node').classed('selected', false);
        this.showEmptyProperties();
    }
    
    showNodeProperties(node) {
        const content = d3.select('#properties-content');
        
        content.html(`
            <div class="property-group">
                <label>Node ID</label>
                <input type="text" id="node-id" value="${node.id}" disabled />
            </div>
            <div class="property-group">
                <label>Node Type</label>
                <select id="node-type">
                    <option value="object" ${node.type === 'object' ? 'selected' : ''}>Object</option>
                    <option value="relation" ${node.type === 'relation' ? 'selected' : ''}>Relation</option>
                    <option value="user" ${node.type === 'user' ? 'selected' : ''}>User</option>
                </select>
            </div>
            <div class="property-group">
                <label>Name</label>
                <input type="text" id="node-name" value="${node.name}" />
            </div>
            <div class="property-group">
                <label>Description</label>
                <textarea id="node-description" rows="3">${node.properties.description || ''}</textarea>
            </div>
            <div class="property-group">
                <h4>Relations</h4>
                <div id="relations-list">
                    ${node.relations.map(rel => `
                        <div class="relation-item">
                            <div class="relation-name">${rel.name}</div>
                            <div class="relation-type">${rel.type}</div>
                        </div>
                    `).join('')}
                </div>
                <button id="add-relation" class="btn">Add Relation</button>
            </div>
        `);
        
        // Event listeners for property changes
        d3.select('#node-name').on('input', (event) => {
            node.name = event.target.value;
            d3.select(`#${node.id} .node-name`).text(node.name);
        });
        
        d3.select('#node-type').on('change', (event) => {
            node.type = event.target.value;
            d3.select(`#${node.id}`)
                .classed('object', node.type === 'object')
                .classed('relation', node.type === 'relation')
                .classed('user', node.type === 'user');
            d3.select(`#${node.id} .node-type`).text(node.type);
        });
        
        d3.select('#node-description').on('input', (event) => {
            node.properties.description = event.target.value;
        });
        
        d3.select('#add-relation').on('click', () => {
            this.addRelation(node);
        });
    }
    
    showEmptyProperties() {
        const content = d3.select('#properties-content');
        content.html('<p>Select an element to edit properties</p>');
    }
    
    addRelation(node) {
        const relationName = prompt('Enter relation name:');
        if (relationName) {
            const relationType = prompt('Enter relation type:');
            if (relationType) {
                node.relations.push({
                    name: relationName,
                    type: relationType
                });
                this.showNodeProperties(node);
            }
        }
    }
    
    loadTemplate(templateName) {
        this.clearCanvas();
        
        if (templateName === 'basic') {
            this.loadBasicTemplate();
        } else if (templateName === 'document') {
            this.loadDocumentTemplate();
        } else if (templateName === 'rbac') {
            this.loadRBACTemplate();
        }
    }
    
    loadBasicTemplate() {
        const user = this.createNode('user', 100, 100, 'User');
        const object = this.createNode('object', 400, 100, 'Object');
        this.createLink(user, object, 'access');
    }
    
    loadDocumentTemplate() {
        const user = this.createNode('user', 100, 100, 'User');
        const group = this.createNode('object', 100, 300, 'Group');
        const doc = this.createNode('object', 400, 100, 'Document');
        const folder = this.createNode('object', 400, 300, 'Folder');
        
        this.createLink(user, doc, 'writer');
        this.createLink(user, group, 'member');
        this.createLink(group, doc, 'reader');
        this.createLink(doc, folder, 'contained_in');
    }
    
    loadRBACTemplate() {
        const user = this.createNode('user', 100, 100, 'User');
        const role = this.createNode('object', 100, 300, 'Role');
        const resource = this.createNode('object', 400, 200, 'Resource');
        
        this.createLink(user, role, 'assigned');
        this.createLink(role, resource, 'access');
    }
    
    validateModel() {
        // Basic validation
        const errors = [];
        
        if (this.nodes.length === 0) {
            errors.push('No nodes in model');
        }
        
        // Check for nodes with no name
        const unnamedNodes = this.nodes.filter(n => !n.name || n.name.trim() === '');
        if (unnamedNodes.length > 0) {
            errors.push(`Found ${unnamedNodes.length} nodes with no name`);
        }
        
        // Check for invalid links
        const invalidLinks = this.links.filter(link => {
            const sourceExists = this.nodes.some(n => n.id === link.source);
            const targetExists = this.nodes.some(n => n.id === link.target);
            return !sourceExists || !targetExists;
        });
        if (invalidLinks.length > 0) {
            errors.push(`Found ${invalidLinks.length} invalid links`);
        }
        
        if (errors.length === 0) {
            alert('Model is valid!');
            return true;
        } else {
            alert('Model validation failed:\n' + errors.join('\n'));
            return false;
        }
    }
    
    saveModel() {
        const model = {
            nodes: this.nodes,
            links: this.links,
            timestamp: new Date().toISOString()
        };
        
        // In a real application, this would save to server
        localStorage.setItem('openfga-model', JSON.stringify(model));
        alert('Model saved!');
    }
    
    exportJSON() {
        const model = {
            nodes: this.nodes,
            links: this.links
        };
        
        const dataStr = JSON.stringify(model, null, 2);
        const dataUri = 'data:application/json;charset=utf-8,' + encodeURIComponent(dataStr);
        
        const exportFileDefaultName = 'openfga-model.json';
        
        const linkElement = document.createElement('a');
        linkElement.setAttribute('href', dataUri);
        linkElement.setAttribute('download', exportFileDefaultName);
        linkElement.click();
    }
    
    clearCanvas() {
        this.svg.selectAll('*').remove();
        this.nodes = [];
        this.links = [];
        this.selectedNode = null;
        this.nodeIdCounter = 0;
        this.linkIdCounter = 0;
        this.showEmptyProperties();
        
        // Recreate arrow marker
        this.svg.append('defs').selectAll('marker')
            .data(['arrow'])
            .enter()
            .append('marker')
            .attr('id', 'arrow')
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 25)
            .attr('refY', 0)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .append('path')
            .attr('d', 'M0,-5L10,0L0,5');
    }
    
    // Drag event handlers
    dragNodeStarted(event, node) {
        this.isDraggingNode = true;
        d3.select(`#${node.id}`).raise();
    }
    
    dragNode(event, node) {
        const transform = d3.zoomTransform(this.canvas.node());
        node.x = event.x / transform.k - transform.x / transform.k;
        node.y = event.y / transform.k - transform.y / transform.k;
        
        d3.select(`#${node.id}`).attr('transform', `translate(${node.x}, ${node.y})`);
        
        // Update all links connected to this node
        this.updateNodeLinks(node);
    }
    
    dragNodeEnded(event, node) {
        this.isDraggingNode = false;
        // Update all links connected to this node
        this.updateNodeLinks(node);
    }
    
    updateNodeLinks(node) {
        this.links.forEach(link => {
            const line = d3.select(`#${link.id}`);
            if (!line.empty()) {
                if (link.source === node.id) {
                    line.attr('x1', node.x + 75).attr('y1', node.y + 80);
                } else if (link.target === node.id) {
                    line.attr('x2', node.x + 75).attr('y2', node.y);
                }
            }
        });
    }
    
    resize() {
        const canvasArea = d3.select('#canvas-area');
        this.width = canvasArea.node().getBoundingClientRect().width;
        this.height = canvasArea.node().getBoundingClientRect().height;
        
        this.svg.attr('width', this.width).attr('height', this.height);
    }
}

// Initialize the editor when the page loads
window.addEventListener('DOMContentLoaded', () => {
    window.modelEditor = new ModelEditor();
});