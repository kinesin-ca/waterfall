<script>
export default {
  props: ['task'],
};
</script>

<template>
  <div id="task-details" v-if="task !== null">
    <details open>
      <summary>Task Details for {{ task.name }}</summary>
        <table>
          <tbody>
            <tr>
              <th>Name</th>
              <td>{{ task.name }}</td>
            </tr>
            <tr>
              <th>State</th>
              <td>{{ task.state }}</td>
            </tr>
            <tr>
              <th>Definition</th>
              <td><pre>{{ JSON.stringify(task.task, null, 2) }}</pre></td>
            </tr>
          </tbody>
        </table>
      <details v-for="attempt in task.attempts" :key="attempt.id" open>
        <summary>Attempt {{attempt.id}}</summary>
        <table>
          <tbody>
            <tr>
              <th>Start Time</th>
              <td>{{ attempt.startTime }}</td>
            </tr>
            <tr>
              <th>Stop Time</th>
              <td>{{ attempt.stopTime }}</td>
            </tr>
            <tr>
              <th>Return Code</th>
              <td>{{ attempt.rc }}</td>
            </tr>
            <tr>
              <th>Standard Out</th>
              <td><pre>{{ attempt.outputLog }}</pre></td>
            </tr>
            <tr>
              <th>Standard Error</th>
              <td><pre>{{ attempt.errorLog }}</pre></td>
            </tr>
            <tr>
              <th>Executor Log</th>
              <td><pre>{{ attempt.ExecutorLog }}</pre></td>
            </tr>
          </tbody>
        </table>
      </details>
    </details>
  </div>
</template>
