<template>
  <v-app>
    <v-main>
      <RouterView />
    </v-main>
  </v-app>
</template>

<script>
export default {

  data() {
        return {   
          var1: "Hola soy var1",
          isAuthenticated: false,
          token: "",
        };
      },

}
</script>


