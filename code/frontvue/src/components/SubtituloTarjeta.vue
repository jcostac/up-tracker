<template>
    <v-card-subtitle :style="{ fontSize: fontSize }">

      
      <div style="padding: 0; margin: 0;">
        <transition name="fade" mode="out-in">
          <i v-if="selectedView === null">*Seleccione una opción de vista para comenzar a consultar datos.</i> 
          <i v-else>{{text}} {{selectedView}}.</i>
        </transition>
      </div>

    </v-card-subtitle>
  </template>
  
  <script>  
  export default {
    name: 'SubtituloTarjeta',
    props: {
      selectedView: {
        type: String,
        default: null
      }, 
      text: {
        type: String,
        default: 'Consultando datos para'
      },
      fontSize: {
        type: String,
        default: '17px'
      }
    }
  }
  </script>
  
  <style scoped>
  /*SUBTITLE*/
  .v-card-subtitle {
    margin: 20px;
    text-align: center;
  }

  /*SUBTITLE TRANSITION*/
  .fade-enter-active, .fade-leave-active {
    transition: all 0.5s ease, all 0.5s ease;
  }

  .fade-enter-from, .fade-leave-to {
    opacity: 0;
    transform: translateX(20px);
  } 
  </style>

