<script>
export default {
  props: ['activeSegment']
};
</script>

<template>
  <div id="segment-details" v-if="activeSegment !== null">
    <table>
      <thead>
        <tr>
          <th>Resource</th>
          <th>Task Name</th>
          <th>Interval</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>{{activeSegment.group}}</td>
          <td>{{activeSegment.label}}</td>
          <td>{{activeSegment.timeRange}}</td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<style>
  .svgicon {
    height: 1em;
    width: auto;
  }
</style>
