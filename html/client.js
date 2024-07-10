// Main app function
const duckdbApp = () => ({
  // Initialize state
  ws: null,
  currentQuery: "",
  output: "",
  previousCommands: [],
  searchTerm: "",
  loading: false,
  commandIndex: -1,
  startTime: 0,
  outputBuffer: "",
  parseJsonTimeout: null,
  tableData: [],
  tableHeaders: [],
  sortColumn: null,
  sortDirection: 'asc',
  filters: {},

  // Initialize WebSocket connection
  initWebSocket() {
    const wsProtocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    this.ws = new WebSocket(`${wsProtocol}//${location.host}`);
    this.ws.onmessage = ({ data }) => {
      this.handleMessage(data);
      this.updateLatestExecutionTime();
      this.loading = false;
    };
  },

  // Handle incoming messages
  handleMessage(data) {
    const [type, ...contentParts] = data.split(':');
    const content = contentParts.join(':');

    if (type === "stdout") {
      this.handleStdout(content);
    } else if (type === "stderr") {
      this.handleStderr(content);
    }
  },

  // Handle stdout messages
  handleStdout(content) {
    this.outputBuffer += content;
    this.appendOutput(content);
    
    if (this.parseJsonTimeout) {
      clearTimeout(this.parseJsonTimeout);
    }

    this.parseJsonTimeout = setTimeout(() => {
      this.tryParseJson();
    }, 300);
  },

  // Try to parse the output buffer as JSON
  tryParseJson() {
    try {
      const jsonData = JSON.parse(this.outputBuffer);
      this.renderTable(jsonData);
      const outputLines = this.output.split('\n');
      outputLines.splice(-this.outputBuffer.split('\n').length);
      this.output = outputLines.join('\n') + '\n' + "Query executed successfully. Results shown in table below.";
    } catch (error) {
      // If parsing fails, do nothing (keep the output as is)
    } finally {
      this.outputBuffer = "";
      this.scrollToBottom();
    }
  },

  // Handle stderr messages
  handleStderr(content) {
    this.appendOutput(`<span class="text-red-500">${content}</span>`);
  },

  // Append to output and trigger re-render
  appendOutput(content) {
    this.output += content;
    this.scrollToBottom();
  },

  // Render table from JSON data
  renderTable(data) {
    if (Array.isArray(data) && data.length > 0) {
      this.tableHeaders = Object.keys(data[0]);
      this.tableData = data;
    } else if (typeof data === 'object' && data !== null) {
      this.tableHeaders = Object.keys(data);
      this.tableData = [data];
    } else {
      this.tableHeaders = [];
      this.tableData = [];
    }
    this.sortColumn = null;
    this.sortDirection = 'asc';
    this.filters = {};
  },

  // Sort table
  sortTable(column) {
    if (this.sortColumn === column) {
      this.sortDirection = this.sortDirection === 'asc' ? 'desc' : 'asc';
    } else {
      this.sortColumn = column;
      this.sortDirection = 'asc';
    }
  },

  // Filter table
  filterTable(column, value) {
    if (value) {
      this.filters[column] = value.toLowerCase();
    } else {
      delete this.filters[column];
    }
  },

  // Computed property for filtered and sorted table data
  get filteredTableData() {
    let data = this.tableData;

    // Apply filters
    Object.keys(this.filters).forEach(column => {
      const filterValue = this.filters[column];
      data = data.filter(row => 
        String(row[column]).toLowerCase().includes(filterValue)
      );
    });

    // Apply sorting
    if (this.sortColumn) {
      data.sort((a, b) => {
        if (a[this.sortColumn] < b[this.sortColumn]) return this.sortDirection === 'asc' ? -1 : 1;
        if (a[this.sortColumn] > b[this.sortColumn]) return this.sortDirection === 'asc' ? 1 : -1;
        return 0;
      });
    }

    return data;
  },

  // Check if query is valid
  isValidQuery() {
    const query = this.currentQuery.trim();
    return query.startsWith(".") || query.endsWith(";");
  },

  // Handle Enter key press
  handleEnterKey(event) {
    if (!event.shiftKey && this.isValidQuery()) {
      this.sendQuery();
    } else {
      const { selectionStart: start, selectionEnd: end, value } = event.target;
      this.currentQuery = `${value.slice(0, start)}\n${value.slice(end)}`;
      setTimeout(() => { event.target.selectionStart = event.target.selectionEnd = start + 1; }, 0);
    }
  },

  // Send query to server
  sendQuery() {
    if (this.isValidQuery() && this.ws) {
      this.startTime = Date.now();
      this.ws.send(this.currentQuery);
      this.appendOutput(`> ${this.currentQuery}\n`);
      this.previousCommands.unshift({ query: this.currentQuery, time: 0 });
      [this.currentQuery, this.loading, this.commandIndex, this.outputBuffer] = ["", true, -1, ""];
      if (this.parseJsonTimeout) {
        clearTimeout(this.parseJsonTimeout);
      }
      this.tableData = [];
      this.tableHeaders = [];
      this.sortColumn = null;
      this.sortDirection = 'asc';
      this.filters = {};
    }
  },

  // Set current query
  setQuery(query) {
    this.currentQuery = query;
  },

  // Navigate command history
  navigateHistory(direction) {
    this.commandIndex += direction === "up" ? 1 : -1;
    this.commandIndex = Math.max(-1, Math.min(this.commandIndex, this.previousCommands.length - 1));
    this.currentQuery = this.commandIndex === -1 ? "" : this.previousCommands[this.commandIndex].query;
  },

  // Update execution time of latest command
  updateLatestExecutionTime() {
    if (this.previousCommands.length) {
      this.previousCommands[0].time = (Date.now() - this.startTime) / 1000;
    }
  },

  // Scroll output to bottom
  scrollToBottom() {
    const outputElement = document.getElementById("output");
    if (outputElement) outputElement.scrollTop = outputElement.scrollHeight;
  },

  // Filtered commands for search functionality
  get filteredCommands() {
    return this.previousCommands.filter(cmd => 
      cmd.query.toLowerCase().includes(this.searchTerm.toLowerCase())
    );
  }
});

// Make duckdbApp available globally for Alpine.js
globalThis.duckdbApp = duckdbApp;