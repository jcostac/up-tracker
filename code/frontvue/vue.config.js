const { defineConfig } = require('@vue/cli-service')
module.exports = defineConfig({
  transpileDependencies: true,

  // Add this section to configure Babel loader
  chainWebpack: config => {
    config.module
      .rule('js')
      .use('babel-loader')
      .tap(options => {
        return options;
      });
  },

  pluginOptions: {
    vuetify: {
      // https://github.com/vuetifyjs/vuetify-loader/tree/next/packages/vuetify-loader
    }
  }
})
