<template>
  <v-card class="d-flex flex-column" height="100%">
    <v-layout class="flex-grow-1">
      <v-app-bar
        color="teal"
        prominent
      >
        <v-app-bar-nav-icon variant="text" @click.stop="drawer = !drawer"></v-app-bar-nav-icon>

        <v-toolbar-title>UP-Tracker</v-toolbar-title>

        <v-spacer></v-spacer>   

        <template v-if="$vuetify.display.mdAndUp">
        </template>

        <router-link to="/login" class="nav-link">
          <v-btn icon="mdi-exit-to-app" variant="text"></v-btn>
        </router-link>

      </v-app-bar>

    <v-app-bar app flat>
            
      <v-container class="mx-auto d-flex align-center justify-center">

        <router-link to="/up" class="nav-link">
          <v-btn rounded="xl" size="x-large" >Consultas por UP</v-btn>          
        </router-link>
        <v-spacer></v-spacer> 

        <router-link to="/uof" class="nav-link">
          <v-btn rounded="xl" size="x-large" >Consultas por UOF</v-btn>          
        </router-link>
        <v-spacer></v-spacer> 

        <router-link to="/precios" class="nav-link">
          <v-btn rounded="xl" size="x-large" >Consultas de precio</v-btn>          
        </router-link>
        <v-spacer></v-spacer> 
        

      </v-container>

    </v-app-bar>


      <v-navigation-drawer
        v-model="drawer"
        :location="$vuetify.display.mobile ? 'bottom' : undefined"
        temporary
      >

        <v-list>



        <v-list-item>
          <router-link to="/simulaciones" class="nav-link">
          <v-icon class="nav-icon">mdi-play-circle</v-icon> <!-- Cambia 'mdi-home' por el ícono que prefieras -->          
          Simulaciones
          </router-link>
          <v-spacer></v-spacer>
        </v-list-item>
        


        </v-list>


      </v-navigation-drawer>

      <!-- EMPTY STATE -->
      <v-main v-if="isRouterViewEmpty" class="empty-state">
          <img :src="require('../assets/logo.png')" alt="Empty State" class="empty-state-image"/>
      </v-main>
      <!-- END EMPTY STATE -->

      <!-- ROUTER VIEW -->
      <transition name="fade" mode="out-in">
      <v-main v-if="!isRouterViewEmpty" :key="$route.fullPath">
        <router-view class="flex-grow-1"></router-view>
      </v-main>
      </transition>
      <!-- END ROUTER VIEW -->

    </v-layout>

    <v-footer app height="auto" class="custom-footer px-0">
        <v-row cols="12" justify="end" align="center" class="ma-0 h-100">
          <v-col cols="auto" class="pa-0">
            <img :src="require('../assets/Simulart Logo.png')" alt="SimularT Logo" class="image-footer"/>
          </v-col>
        </v-row>
    </v-footer>
  </v-card>
</template>
<script>
  import store from '../store';
  export default {
    data: () => ({
      usuario: store.state.token,
      drawer: false,
      group: null,
      items: [
        {
          title: 'Foo',
          value: 'foo',
        },
        {
          title: 'Bar',
          value: 'bar',
        },
        {
          title: 'Fizz',
          value: 'fizz',
        },
        {
          title: 'Buzz',
          value: 'buzz',
        },
      ],
    }),

    watch: {
      group () {
        this.drawer = false
      },
    },


    computed: {
    isRouterViewEmpty() {
      // Determina si el router-view está vacío

      console.log("nombre:")
      console.log(this.$route.name)
      console.log(!this.$route.name)

      if (this.$route.name == 'PaginaHome') {
          return true
      } else {
          return false
      }
    }
    }


  }
</script>


<style scoped>

/* Card */
.v-card {
  display: flex;
  flex-direction: column;
}

/* Navbar */
.nav-link {
  text-decoration: none;
  color: inherit; /* Heredar el color del texto padre */
  display: flex;
  align-items: center; /* Alinear ícono y texto verticalmente */
}

.nav-icon {
  margin-right: 8px; /* Espacio entre el ícono y el texto */
}

/* Main */
.v-main {
  height: 100%;
  display: flex;
  justify-content: center;
  align-items: center;
  background-color: transparent;
}


/* Empty State */
.empty-state {
  justify-content: center;
  align-items: center;
}

.empty-state-image {
  width: 500px;
  height: auto;
  display: block;
  max-width: 500px; 
  margin: 0 auto;
  opacity: 10%;
}

/* Footer */

.image-footer {
  height: 60px;
  width: auto;
  padding-bottom: 0px;
  padding-right: 20px;
}

.v-footer {
  max-height: 100px;
  background-color: transparent;
}

/*ENTER FADE ANIMATION*/
.fade-enter-from {
  opacity: 0;
  transform: translateY(80px);
}

.fade-enter-active {
  transition: all 1s ease;
}

/*LEAVE FADE ANIMATION*/
.fade-leave-from {
  opacity: 1;
  transform: translateY(0px);
}

.fade-leave-active {
  transition: all 1s ease;
}


</style>