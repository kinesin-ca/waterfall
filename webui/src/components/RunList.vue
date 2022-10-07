<script>
import { ALL_STATES, defaultCountHandler } from '../defs.js'

import SortableTableHeader from './SortableTableHeader.vue';

// import SortIndicator from './SortIndicator.vue';
// import RunButton from './RunButton.vue';
// components: { RunButton, SortIndicator },

export default {
  props: ['daggydURL', 'refreshSeconds'],
  components: { SortableTableHeader },
  data() {
    return {
      sortCol: 'lastUpdate', // Which column to sort view on
      sortAscending: false,
      runs: [],
      filterStates: ALL_STATES.map((x) => x.name),
      filterMinTime: 0,
      filterMaxTime: 2000000000000000000,
      filterRegex: '.*',
      columns: [
        { name: 'runID', title: 'Run ID', sortable: true },
        { name: 'tag', title: 'Tag', sortable: true },
        { name: 'state', title: 'State', sortable: true },
        { name: 'progress', title: 'Progress', sortable: true },
        { name: 'startTime', title: 'Start Time', sortable: true },
        { name: 'lastUpdate', title: 'LastUpdate', sortable: true },
        { name: 'queued', title: 'Queued', sortable: true },
        { name: 'running', title: 'Running', sortable: true },
        { name: 'errored', title: 'Errored', sortable: true },
        { name: 'completed', title: 'Completed', sortable: true },
        { name: 'controls', title: 'Controls', sortable: false },
      ],
    };
  },
  computed: {
    runList() {
      return this.runs
        .filter((run) => this.runFilter(run))
        .map((r) => {
          const run = r;
          run.nTasks = Object
            .values(run.taskCounts)
            .reduce((prev, cur) => prev + cur, 0);
          run.task_states = new Proxy(run.taskCounts, defaultCountHandler);
          run.progress = run.task_states.COMPLETED / run.nTasks;
          return run;
        })
        .sort((a, b) => this.sorter(a, b));
    },
    allStates() {
      return ALL_STATES;
    },
  },

  watch: {
    refreshSeconds() {
      this.fetchRuns();
    },
    daggydURL() {
      this.fetchRuns();
    },
    activeRunID() {
      this.fetchRuns();
    },
  },

  methods: {
    isNumeric(x) {
      const p = parseFloat(x);
      return !Number.isNaN(p) && Number.isFinite(p);
    },

    sorter(a, b) {
      const aa = a[this.sortCol];
      const bb = b[this.sortCol];

      let ret = 0;
      if (this.isNumeric(aa) && this.isNumeric(bb)) {
        ret = aa - bb;
      } else if (aa < bb) {
        ret = -1;
      } else if (bb === aa) {
        ret = 0;
      } else {
        ret = 1;
      }

      if (!this.sortAscending) {
        ret *= -1;
      }
      return ret;
    },

    runFilter(run) {
      const reFilter = new RegExp(this.filterRegex, '');
      return (this.filterStates.indexOf(run.state) > -1)
        && (run.startTime >= this.filterMinTime)
        && (run.lastUpdate <= this.filterMaxTime)
        && (reFilter.test(run.tag));
    },

    setSortCol(name) {
      if (this.sortCol === name) {
        this.sortAscending = !this.sortAscending;
      } else {
        this.sortCol = name;
        this.sortAscending = true;
      }
    },

    killRun(runID) {
      fetch(`${this.daggydURL}/v1/dagrun/${runID}`, { method: 'delete' });
    },

    retryRun(runID) {
      fetch(`${this.daggydURL}/v1/dagrun/${runID}/state/QUEUED`, { method: 'patch' });
    },

    async fetchRuns() {
      const res = await fetch(`${this.daggydURL}/v1/dagruns?all=1`);
      this.runs = await res.json();
    },

    update() {
      this.fetchRuns();
      setTimeout(() => {
        this.update();
      }, this.refreshSeconds * 1000);
    },
  },

  mounted() {
    this.update();
  },
};
</script>

<template>
  <div id="run-list">
    <div id="run-list-filter">
      <details>
        <summary>Run Filter</summary>
          <div>
            <label>
              Start Time
              <input v-model.lazy="filterMinTime"/>
            </label>
            <label>
              Time
              <input v-model.lazy="filterMaxTime"/>
            </label>
            <label>
              Task Name Regex
              <input v-model.lazy="filterRegex"/>
            </label>
          </div>
          <div>
            <label v-for="state in allStates" :key="state.name">
              {{ state.display }}
              <input type="checkbox" :value="state.name" v-model="filterStates">
            </label>
          </div>
      </details>
    </div>
    <div id="run-list-data">
      <table class="table">
        <thead>
          <tr>
            <th v-for="col in columns" :key="col.name">
              <SortableTableHeader
                :title="col.title"
                :sorted="col.name == this.sortCol"
                :ascending="sortAscending"
                :sortable="col.sortable"
                @update-sort-column="setSortCol(col.name)"
                />
            </th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="run in runList" :key="run.runID">
            <td>{{run.runID}}</td>
            <td>{{run.tag}}</td>
            <td>{{run.state}}</td>
            <td><progress :value="run.progress"></progress></td>
            <td>{{run.startTime}}</td>
            <td>{{run.lastUpdate}}</td>
            <td>{{run.task_states["QUEUED"]}}</td>
            <td>{{run.task_states["RUNNING"]}}</td>
            <td>{{run.task_states["ERRORED"]}}</td>
            <td>{{run.task_states["COMPLETED"]}}</td>
            <td>
                <a href="#">
                  <img
                    class='svgicon'
                    src='/icon-search.svg'
                    @click="$emit('update-active-runid', run.runID)"/>
                </a>
                <a href="#">
                  <img class='svgicon' src='/icon-trash.svg' @click="killRun(run.runID)"/>
                </a>
                <a href="#">
                  <img class='svgicon' src='/icon-launch.svg' @click="retryRun(run.runID)"/>
                </a>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<style>
  .svgicon {
    height: 1em;
    width: auto;
  }
</style>
