// Relationship Visualizer Application
class RelationshipVisualizer {
    constructor() {
        this.width = 0;
        this.height = 0;
        this.svg = null;
        this.simulation = null;
        this.nodes = [];
        this.links = [];
        this.nodeElements = null;
        this.linkElements = null;
        this.selectedNode = null;
        this.highlightedPath = [];
        
        this.init();
        this.setupEventListeners();
        this.loadSampleData();
    }
    
    init() {
        const container = d3.select('#graph-container');
        const svg = d3.select('#graph');
        
        // Set up dimensions
        this.width = container.node().getBoundingClientRect().width;
        this.height = container.node().getBoundingClientRect().height;
        
        svg.attr('width', this.width).attr('height', this.height);
        
        // Add zoom behavior
        const zoom = d3.zoom()
            .scaleExtent([0.1, 4])
            .on('zoom', (event) => {
                g.attr('transform', event.transform);
            });
        
        svg.call(zoom);
        
        // Create a group for all graph elements
        this.g = svg.append('g');
        
        // Create arrow marker
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
        
        this.svg = svg;
    }
    
    setupEventListeners() {
        // Search functionality
        d3.select('#search-input').on('input', (event) => {
            this.searchNodes(event.target.value);
        });
        
        // Zoom controls
        d3.select('#zoom-in').on('click', () => {
            this.zoom(1.2);
        });
        
        d3.select('#zoom-out').on('click', () => {
            this.zoom(0.8);
        });
        
        d3.select('#reset-zoom').on('click', () => {
            this.resetZoom();
        });
        
        d3.select('#center-graph').on('click', () => {
            this.centerGraph();
        });
        
        // Window resize
        window.addEventListener('resize', () => {
            this.resize();
        });
    }
    
    loadSampleData() {
        // Sample data for testing
        this.nodes = [
            { id: 'user:alice', type: 'user', label: 'Alice' },
            { id: 'user:bob', type: 'user', label: 'Bob' },
            { id: 'document:doc1', type: 'document', label: 'Document 1' },
            { id: 'document:doc2', type: 'document', label: 'Document 2' },
            { id: 'folder:folder1', type: 'folder', label: 'Folder 1' },
            { id: 'group:engineering', type: 'group', label: 'Engineering' }
        ];
        
        this.links = [
            { source: 'user:alice', target: 'document:doc1', relation: 'writer' },
            { source: 'user:alice', target: 'group:engineering', relation: 'member' },
            { source: 'user:bob', target: 'document:doc2', relation: 'reader' },
            { source: 'group:engineering', target: 'document:doc1', relation: 'reader' },
            { source: 'group:engineering', target: 'document:doc2', relation: 'writer' },
            { source: 'document:doc1', target: 'folder:folder1', relation: 'contained_in' },
            { source: 'document:doc2', target: 'folder:folder1', relation: 'contained_in' }
        ];
        
        this.render();
    }
    
    render() {
        // Create or update links
        this.linkElements = this.g.selectAll('.link')
            .data(this.links)
            .join('line')
            .attr('class', 'link')
            .attr('marker-end', 'url(#arrow)');
        
        // Create or update nodes
        this.nodeElements = this.g.selectAll('.node')
            .data(this.nodes)
            .join('circle')
            .attr('class', d => `node ${d.type}`)
            .attr('r', 20)
            .attr('fill', d => this.getNodeColor(d.type))
            .on('click', (event, d) => this.selectNode(d))
            .call(d3.drag()
                .on('start', (event, d) => this.dragStarted(event, d))
                .on('drag', (event, d) => this.dragged(event, d))
                .on('end', (event, d) => this.dragEnded(event, d)));
        
        // Add node labels
        const labels = this.g.selectAll('.node-label')
            .data(this.nodes)
            .join('text')
            .attr('class', 'node-label')
            .text(d => d.label)
            .attr('x', 0)
            .attr('y', 40)
            .attr('text-anchor', 'middle');
        
        // Set up simulation
        this.simulation = d3.forceSimulation(this.nodes)
            .force('link', d3.forceLink(this.links).id(d => d.id).distance(100))
            .force('charge', d3.forceManyBody().strength(-300))
            .force('center', d3.forceCenter(this.width / 2, this.height / 2))
            .force('collision', d3.forceCollide().radius(30));
        
        // Update positions on each tick
        this.simulation.on('tick', () => {
            this.linkElements
                .attr('x1', d => d.source.x)
                .attr('y1', d => d.source.y)
                .attr('x2', d => d.target.x)
                .attr('y2', d => d.target.y);
            
            this.nodeElements
                .attr('cx', d => d.x)
                .attr('cy', d => d.y);
            
            labels
                .attr('x', d => d.x)
                .attr('y', d => d.y + 30);
        });
    }
    
    getNodeColor(type) {
        const colors = {
            user: '#1f77b4',
            document: '#ff7f0e',
            folder: '#2ca02c',
            group: '#d62728',
            role: '#9467bd',
            object: '#8c564b'
        };
        return colors[type] || '#7f7f7f';
    }
    
    selectNode(node) {
        // Deselect previous selection
        this.nodeElements.classed('selected', false);
        
        // Select new node
        this.nodeElements.filter(d => d.id === node.id).classed('selected', true);
        this.selectedNode = node;
        
        // Highlight paths from this node
        this.highlightPaths(node);
    }
    
    highlightPaths(startNode) {
        // Clear previous highlights
        this.linkElements.classed('highlighted', false);
        
        // Find all reachable nodes
        const visited = new Set();
        const queue = [startNode.id];
        const highlightedLinks = [];
        
        while (queue.length > 0) {
            const currentId = queue.shift();
            if (visited.has(currentId)) continue;
            visited.add(currentId);
            
            // Find all outgoing links
            this.links.forEach(link => {
                if (link.source.id === currentId || link.source === currentId) {
                    highlightedLinks.push(link);
                    const targetId = link.target.id || link.target;
                    if (!visited.has(targetId)) {
                        queue.push(targetId);
                    }
                }
            });
        }
        
        // Highlight the links
        this.linkElements.filter(d => highlightedLinks.includes(d)).classed('highlighted', true);
        this.highlightedPath = highlightedLinks;
    }
    
    searchNodes(query) {
        const filteredNodes = this.nodes.filter(node => 
            node.label.toLowerCase().includes(query.toLowerCase()) || 
            node.id.toLowerCase().includes(query.toLowerCase())
        );
        
        // Highlight matching nodes
        this.nodeElements.classed('selected', d => filteredNodes.includes(d));
    }
    
    zoom(factor) {
        this.svg.transition().duration(350).call(
            d3.zoom().scaleBy, factor
        );
    }
    
    resetZoom() {
        this.svg.transition().duration(750).call(
            d3.zoom().transform, d3.zoomIdentity
        );
    }
    
    centerGraph() {
        const bounds = this.g.node().getBBox();
        const fullWidth = this.width;
        const fullHeight = this.height;
        const width = bounds.width;
        const height = bounds.height;
        const midX = bounds.x + width / 2;
        const midY = bounds.y + height / 2;
        
        const scale = Math.min(fullWidth / width, fullHeight / height) * 0.8;
        const translate = [fullWidth / 2 - scale * midX, fullHeight / 2 - scale * midY];
        
        this.svg.transition().duration(750).call(
            d3.zoom().transform, d3.zoomIdentity.translate(translate[0], translate[1]).scale(scale)
        );
    }
    
    resize() {
        const container = d3.select('#graph-container');
        this.width = container.node().getBoundingClientRect().width;
        this.height = container.node().getBoundingClientRect().height;
        
        this.svg.attr('width', this.width).attr('height', this.height);
        
        // Update simulation center
        this.simulation.force('center', d3.forceCenter(this.width / 2, this.height / 2));
        this.simulation.alpha(0.3).restart();
    }
    
    // Drag event handlers
    dragStarted(event, d) {
        if (!event.active) this.simulation.alphaTarget(0.3).restart();
        d.fx = d.x;
        d.fy = d.y;
    }
    
    dragged(event, d) {
        d.fx = event.x;
        d.fy = event.y;
    }
    
    dragEnded(event, d) {
        if (!event.active) this.simulation.alphaTarget(0);
        d.fx = null;
        d.fy = null;
    }
    
    // API for external data loading
    loadData(nodes, links) {
        this.nodes = nodes;
        this.links = links;
        this.render();
    }
}

// Initialize the visualizer when the page loads
window.addEventListener('DOMContentLoaded', () => {
    window.visualizer = new RelationshipVisualizer();
});