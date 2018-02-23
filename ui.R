library(shiny)
library(rsconnect)
library(shinydashboard)
library(rclipboard)
library(shinythemes)

rsconnect::setAccountInfo(name='seuriously', token='DC900D5D4906D31ED595C388681A695D', secret='SucQj3x2d3A1kpqHfPgleZ+rn1WI053MHKPyJHzH')

dashboardPage(
  
  skin="black",
  
  dashboardHeader(
    title = ("Teradata SQL Creator"),
    titleWidth = 250
    
  ),
  
  
  dashboardSidebar(
    width = 250,
    sidebarMenu(
      menuItem("Descriptive\nStats", tabName = "descstats", icon = icon("dashboard")),
      menuItem("Feature\nCreator", icon = icon("th"), tabName = "feature",
               badgeLabel = "new", badgeColor = "green")
    )
  ),
  
  
  dashboardBody(
    tags$head(tags$style(HTML('
                              .form-control.shiny-bound-input{
                              height: 50px;
                              font-size: 24px;
                              border: 3px solid #ccc;
                              }
                              
                              '))),
    tabItems(
      tabItem(tabName = "descstats",
        fluidRow(
          box(
            status = "primary",
            width = 12,
            textInput("colnme", placeholder = "Insert variables", label = NULL),
            br(),
            textInput("tablenm", placeholder = "Source Table Name", label = NULL),
            br(),
            textInput("table_target", placeholder = "Target Table Name", label = NULL),
            br(),
            textInput("add_where", placeholder = "Additional Where", label = NULL),
            br(),
            rclipboardSetup(),
            uiOutput("clip"),
            br(),
            br(),
            verbatimTextOutput("result")
          )
        )
      ),
      
      tabItem(tabName = "feature",
        fluidRow(
          box(
            status = "primary",
            width = 12,
            splitLayout(
              textInput("prim_key", placeholder = "Primary Key Column", label = NULL),
              textInput("period_time", placeholder = "Time/Period Column", label = NULL)
            ),
            splitLayout(
              textInput("monthly_tname", placeholder = "Table Source", label = NULL),
              textInput("target_table", placeholder = "Target Table Name", label = NULL)
            ),
            textInput("col_name", placeholder = "Insert Numeric Variables", label = NULL),
            br(),
            textInput("col_name_date", placeholder = "Insert Date Variables", label = NULL),
            rclipboardSetup(),
            uiOutput("clip_feature"),
            br(),
            br(),
            verbatimTextOutput("result_feature")
          )
        )
      )
    )
  )
)

