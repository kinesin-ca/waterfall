<script>
import { ALL_STATES } from '../defs.js'

import SortableTableHeader from './SortableTableHeader.vue';
import TaskDetails from './TaskDetails.vue';

export default {
  props: ['daggydURL', 'refreshSeconds', 'activeRunID'],
  components: { SortableTableHeader, TaskDetails },
  data() {
    return {
      sortCol: 'lastUpdate',
      sortAscending: false,
      run: null,
      activeTaskName: null,
      filterStates: ALL_STATES.map((x) => x.name),
      filterMinTime: 0,
      filterMaxTime: 2000000000000000000,
      filterRegex: '.*',
      columns: [
        { name: 'name', title: 'Name', sortable: true },
        { name: 'state', title: 'State', sortable: true },
        { name: 'startTime', title: 'Last Update', sortable: true },
        { name: 'duration', title: 'Duration (s)', sortable: true },
        { name: 'attempts', title: '# of Attempts', sortable: true },
        { name: 'controls', title: 'Controls', sortable: false },
      ],
    };
  },

  watch: {
    refreshSeconds() {
      this.fetchRun();
    },
    daggydURL() {
      this.fetchRun();
    },
    activeRunID() {
      this.fetchRun();
    },
  },

  computed: {
    tasks() {
      if (this.run === null) {
        return [];
      }
      const tasks = Object.keys(this.run.tasks)
        .map((taskName) => {
          let startTime = 0;
          let stopTime = 0;
          let duration = 0;
          const attempts = (taskName in this.run.taskAttempts
            ? this.run.taskAttempts[taskName]
            : []);
          if (attempts.length > 0) {
            const firstAttempt = attempts[0];
            const lastAttempt = attempts[attempts.length - 1];
            startTime = firstAttempt.startTime;
            stopTime = lastAttempt.stopTime;
            duration = lastAttempt.stopTime - firstAttempt.startTime;
          }

          return {
            name: taskName,
            state: this.run.taskStates[taskName],
            startTime,
            lastUpdate: stopTime,
            attempts: attempts.length,
            duration: (duration / 1e9).toFixed(2),
          };
        });
      return tasks
        .filter(this.filter)
        .sort(this.sorter);
    },
    allStates() {
      return ALL_STATES;
    },
    activeTask() {
      if (this.activeTaskName === null) {
        return null;
      }
      const name = this.activeTaskName;
      const attempts = (name in this.run.taskAttempts ? this.run.taskAttempts[name] : []);
      const augAttempts = attempts
        .sort((a, b) => a.startTime - b.startTime)
        .map((a, i) => {
          a.id = i + 1;
          return a;
        });
      const obj = {
        name,
        task: this.run.tasks[name],
        attempts: augAttempts,
        state: this.run.taskStates[name],
      };
      return obj;
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

    filter(task) {
      const reFilter = new RegExp(this.filterRegex, '');
      return (this.filterStates.indexOf(task.state) > -1)
        && (task.startTime >= this.filterMinTime)
        && (task.lastUpdate <= this.filterMaxTime)
        && (reFilter.test(task.name));
    },

    setSortCol(name) {
      if (this.sortCol === name) {
        this.sortAscending = !this.sortAscending;
      } else {
        this.sortCol = name;
        this.sortAscending = true;
      }
    },

    // Root tags
    // runID, tag, tasks, taskStates, taskAttempts

    async fetchRun() {
      if (this.activeRunID === null) { return; }
      const resp = await fetch(`${this.daggydURL}/v1/dagrun/${this.activeRunID}`);
      this.run = await resp.json();
    },

    killTask(taskName) {
      fetch(`${this.daggydURL}/v1/dagrun/${this.activeRunID}/task/${taskName}`, { method: 'delete' });
    },

    retryTask(taskName) {
      fetch(`${this.daggydURL}/v1/dagrun/${this.activeRunID}/task/${taskName}/state/QUEUED`, { method: 'patch' });
    },

    update() {
      this.fetchRun();
      setTimeout(() => {
        this.update();
      }, this.refreshSeconds * 1000);
    },
    setActiveTask(taskName) {
      this.activeTaskName = taskName;
    },
  },

  mounted() {
    this.update();
  },
};
</script>

<style>
input {
  max-width: 25%;
}
label {
  margin: 5px;
}
</style>

<template>
  <div class="run-view">
    <TaskDetails :task="activeTask" />
    <div id="run-view-filter">
      <details>
        <summary>Task Filter</summary>
          <div>
            <label>
              Min Time
              <input v-model.lazy="filterMinTime"/>
            </label>
            <label>
              Max Time
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
    <div id="run-view-data">
      <table>
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
        <tr v-for="task in tasks" :key="task.name">
          <td>{{task.name}}</td>
          <td>{{task.state}}</td>
          <td>{{task.startTime}}</td>
          <td>{{task.duration}}</td>
          <td>{{task.attempts}}</td>
          <td>
              <img class='svgicon'
                src='/icon-search.svg'
                @click="setActiveTask(task.name)"/>
              <img class='svgicon' src='/icon-trash.svg' @click="killTask(task.name)"/>
              <img class='svgicon' src='/icon-launch.svg' @click="retryTask(task.name)"/>
          </td>
        </tr>
      </tbody>
      </table>
    </div>
  </div>
</template>
