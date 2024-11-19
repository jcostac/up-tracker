import { createApp } from 'vue'
import { createRouter, createWebHistory } from 'vue-router';
import App from './App.vue'
import vuetify from './plugins/vuetify'
import { loadFonts } from './plugins/webfontloader'
import store from './store';
import VueDatePicker from '@vuepic/vue-datepicker';
import '@vuepic/vue-datepicker/dist/main.css';
import Toast from "vue-toastification";
import "vue-toastification/dist/index.css";

import VueSweetalert2 from 'vue-sweetalert2';
import 'sweetalert2/dist/sweetalert2.min.css';

import PaginaHome from './components/PaginaHome.vue';
import PaginaLogin from './components/PaginaLogin.vue';
import ComponenteUP from './components/ComponenteUP.vue';
import ComponenteUOF from './components/ComponenteUOF.vue';
import ComponentePrecios from './components/ComponentePrecios.vue';

loadFonts()

const routes = [  
  { path: '/login', name: 'PaginaLogin', component: PaginaLogin },
  { path: '/', name: 'PaginaHome', component: PaginaHome, meta: { requiresAuth: true } ,
    children: [      
      { path: '/up', name: 'ComponenteUP', component: ComponenteUP, meta: { requiresAuth: true } },
      { path: '/uof', name: 'ComponenteUOF', component: ComponenteUOF, meta: { requiresAuth: true } },
      { path: '/precios', name: 'ComponentePrecios', component: ComponentePrecios, meta: { requiresAuth: true } },
    ]
  },
  
];

const router = createRouter({
  history: createWebHistory(),  
  routes: routes,
});


const app = createApp(App)

const options = {
  position: 'top-center',
  timeout: 5000,
  closeOnClick: true,
  pauseOnHover: true,
  draggable: true,
  draggablePercent: 0.6,
  showCloseButtonOnHover: false,
  hideProgressBar: true,
  closeButton: 'button',
  icon: true,
  rtl: false
};


router.beforeEach((to, from, next) => {  

  const isAuthenticated = store.state.usuarioAutenticado; 

  if (to.matched.some(record => record.meta.requiresAuth) && !isAuthenticated) {             
    console.log("A Login")
    next('/login')
  } else {
    console.log("A Home")
    next()
  }
});



//createApp(App)
app.use(VueSweetalert2);
app.use(Toast, options);
app.use(router)
app.use(vuetify)
app.use(store)
app.component('VueDatePicker', VueDatePicker);
app.mount('#app')