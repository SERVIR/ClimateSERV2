/**
 * Clones the carousel item template and replaces the fields with
 * item attributes then returns the html.
 * @param {object} item - Data item object (data-item.js)
 * @returns html
 */
function getItemHtml(item) {
  var replica = $("#dataCarouselItemsTemplate:first").clone();
  return replica
    .html()
    .replace("{title}", item.title)
    .replace("{description}", item.text)
    .replace("{imagesrc}", static_url + 'frontend/' + item.image.src )
    .replace("{imagealt}", "'" + item.image.alt + "'")
    .replaceAll("{buttontext}", item.button.text)
    .replace("{metaid}", item.metedata)
    .replace("{buttonlocation}", "'" + item.button.location + "'");
}

/**
 * Sets the active “you can do” slide
 * @param {int} which - index of the slide to activate
 */
function setYouCanDoSlide(which) {
  $("#usageCarousel").carousel(which);
}

/**
 * Ensures that all data teaser cards height match the height of the highest card for uniformity.
 */
function adjustCards() {
  $(".card").animate({ height: "auto" }, function () {
    $(".card").height("auto");
    var maxHeight = Math.max.apply(
      null,
      $(".card")
        .map(function () {
          return $(this).height();
        })
        .get()
    );
    $(".card").height(maxHeight);
  });
}

/**
 * Calls adjustCards on any window resize
 */
$(window)
  .resize(function () {
    adjustCards();
  })
  .resize();

/**
 * Sets up the carousels and loads data into the teaser from the dataItems structure.
 * It also creates cloned copies of the slides and mashes them into new slides that
 * contain three slides each in order to have the three slides showing.
 */
function initIndex() {
  $("#main-carousel").carousel();
  $("#usageCarousel").carousel({
    interval: false,
  });
  $.each(dataItems, function (index, item) {
    var active = index === 0;
    if (index === 0) {
      $("#dataCarouselItems").append(
        getItemHtml(item).replace(
          "carousel-item h-100",
          "carousel-item active h-100"
        )
      );
    } else {
      $("#dataCarouselItems").append(getItemHtml(item));
    }
  });

  $("#dataCarousel").carousel({
    interval: 2000,
    transition: "fade-in",
  });

  $("#dataCarousel.carousel .carousel-item").each(function () {
    var minPerSlide = 3;
    var next = $(this).next();
    if (!next.length) {
      next = $(this).siblings(":first");
    }
    next.children(":first-child").clone().appendTo($(this));

    for (var i = 0; i < minPerSlide; i++) {
      next = next.next();
      if (!next.length) {
        next = $(this).siblings(":first");
      }

      next.children(":first-child").clone().appendTo($(this));
    }
  });

  adjustCards();
}

/**
 * Calls initIndex on ready
 *
 * @event index-ready
 */
$(function () {
  initIndex();
});
