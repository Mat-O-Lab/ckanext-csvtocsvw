/* Polls the status endpoint of an annotate/transform page and renders the job log
   as a timeline (same look as ckanext-csvwmapandtransform). */
ckan.module("csvtocsvw", function ($) {
  "use strict";
  return {
    initialize: function () {
      var self = this;
      var apiUrl = this.el.get(0).getAttribute("data-api-url");
      if (!apiUrl) {
        return;
      }
      var shown = 0;

      function render(entry) {
        var li = $("<li>").addClass("item " + entry["class"]);
        li.append($("<i>").addClass("fa icon fa-" + entry.icon));
        li.append(
          $("<div>")
            .addClass("alert alert-" + entry.alertlevel + " mb-0 mt-3")
            .attr("role", "alert")
            .text(entry.message)
        );
        li.append($("<span>").addClass("date").text(entry.timestamp));
        return li;
      }

      function update() {
        $.ajax({
          url: apiUrl,
          type: "GET",
          dataType: "json",
          success: function (data) {
            var status = (data && data.status) || {};
            if (status.status) {
              self.el.find('td[name="status-value"]').text(status.status);
            }
            var logs = status.logs || [];
            if (logs.length) {
              self.el.find('div[name="status"]').removeClass("invisible");
            }
            if (logs.length !== shown) {
              // job log is replaced when a new job starts: re-render from scratch
              var list = self.el.find('ul[name="log"]').empty();
              $.each(logs, function (_i, entry) {
                list.append(render(entry));
              });
              shown = logs.length;
            }
          },
          complete: function () {
            setTimeout(update, 2000);
          },
        });
      }
      update();
    },
  };
});
