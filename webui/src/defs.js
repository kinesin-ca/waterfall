import { reactive } from 'vue';

export const ALL_STATES = [
  { name: 'QUEUED', display: 'Queued' },
  { name: 'RUNNING', display: 'Running' },
  { name: 'ERRORED', display: 'Errored' },
  { name: 'COMPLETED', display: 'Completed' },
  { name: 'KILLED', display: 'Killed' },
];

export const defaultCountHandler = {
  get(target, name) {
    return name in target ? target[name] : 0;
  },
};


