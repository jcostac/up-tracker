// store/index.js
import { createStore } from 'vuex';

export default createStore({
  state: {
    usuarioAutenticado: null,
    token: null,
    usuarioID: null,
    usuarionNombre: null,
  },
  mutations: {
    setUsuarioAutenticado(state, value) {
      state.usuarioAutenticado = value;
    },
    setToken(state, value) {
        state.token = value;
    },
    setUsuarioID(state, value) {
        state.usuarioID = value;
    },    
    setUsuarioNombre(state, value) {
        state.usuarioNombre = value;
    },
  },
  actions: {
    actualizarUsuarioAutenticado({ commit }, value) {
      commit('setUsuarioAutenticado', value);
    },
    actualizarToken({ commit }, value) {
        commit('setToken', value);
      },
    actualizarUsuarioID({ commit }, value) {
        commit('setUsuarioID', value);
    },
    actualizarUsuarioNombre({ commit }, value) {
        commit('setUsuarioNombre', value);
    },
      

  },
  getters: {
    getUsuarioAutenticado: state => state.usuarioAutenticado,
    getToken: state => state.token,
    getUsuarioID: state => state.usuarioID,
    getUsuarioNombre: state => state.usuarioNombre,
  }
});
